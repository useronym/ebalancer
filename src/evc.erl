%%%-------------------------------------------------------------------
%%% @author xtovarn
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. IV 2015 9:24
%%%-------------------------------------------------------------------
-module(evc).
-author("xtovarn").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_SIZE, 4).

%% API
-export([perf1/0, new/1, increment/1, node_id/1, counter/1, counter/2, merge/2, compare/2, descends/2]).

-type timestamp() :: integer().
-type evc() :: {list(), timestamp(), integer()}.

-spec new(1..99) -> evc().
new(NodeId) ->
	new(NodeId, timestamp()).

new(NodeId, NodeTime) ->
	{set_nth(NodeId, {0, NodeTime}, [{-1, 0} || _ <- lists:seq(1, ?DEFAULT_SIZE)]), 0, NodeId}.

-spec increment(evc()) -> evc().
increment(VC) ->
	increment(timestamp(), VC).

increment(NodeTime, {VCList, TA, NodeId}) ->
	{Counter, LastNodeTime} = lists:nth(NodeId, VCList),
	TimeShift = NodeTime - LastNodeTime,
	{set_nth(NodeId, {Counter + 1, NodeTime}, VCList), TA + TimeShift, NodeId}.

-spec node_id(evc()) -> integer().
node_id({_, _, NodeId}) ->
    NodeId.

-spec counter(evc()) -> integer().
counter(VC = {_, _, NodeId}) ->
	counter(NodeId, VC).

-spec counter(integer(), evc()) -> integer().
counter(NodeId, {VCList, _, _}) ->
	{Counter, _} = lists:nth(NodeId, VCList),
	Counter.

-spec merge(evc(), evc()) -> evc().
merge(VC1, VC2) ->
	merge(timestamp(), VC1, VC2, 0).

merge(NodeTime, {LocalVCList, LocalTA, NodeId}, {RemoteVCList, RemoteTA, _}, RTTDelta) ->
	{Counter, LastNodeTime} = lists:nth(NodeId, LocalVCList),
	UpdatedLocalVCList = set_nth(NodeId, {Counter, NodeTime}, LocalVCList),
	Merge = lists:zipwith(fun max/2, UpdatedLocalVCList, RemoteVCList),
	TimeShift = NodeTime - LastNodeTime,
	{Merge, approximate_ta(LocalTA, RemoteTA, TimeShift, RTTDelta), NodeId}.

approximate_ta(LocalTA, RemoteTA, TimeShift, RTTDelta) ->
	((LocalTA + TimeShift) + (RemoteTA + RTTDelta)) div 2.

-spec descends(evc(), evc()) -> boolean().
descends({VCList1, _, _}, {VCList2, _, _}) ->
	descends_2(VCList1, VCList2).

descends_2([], []) ->
	true;
descends_2([{H1, _} | T1], [{H2, _} | T2]) when H1 >= H2 ->
	descends_2(T1, T2);
descends_2(_, _) ->
	false.

-spec compare(evc(), evc()) -> boolean().
compare(VC1, VC2) ->
	case descends(VC2, VC1) of
		true -> true;
		_ ->
			case descends(VC1, VC2) of
				true -> false;
				_ -> compare_decide(VC1, VC2)
			end
	end.

compare_decide({_, TA1, NId1}, {_, TA2, NId2}) when {TA1, NId1} > {TA2, NId2} ->
	false;
compare_decide(_, _) ->
	true.

%% ===================================================================
%% Private functions
%% ===================================================================

timestamp() ->
	{MegaSecs, Secs, MicroSecs} = erlang:now(),
	MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.

set_nth(1, Element, [_ | T]) -> [Element | T];
set_nth(N, Element, [H | T]) -> [H | set_nth(N - 1, Element, T)].

%% ===================================================================
%% Tests
%% ===================================================================

perf1() ->
	Node1_VC1 = evc:new(1),
	timer:sleep(500),
	Node1_VC2 = {_, TA1, _} = evc:increment(Node1_VC1),
	Node4_VC1 = merge(timestamp(), new(4, timestamp()), Node1_VC2, TA1),
	timer:sleep(500),
	Node1_VC3 = evc:increment(evc:increment(Node1_VC2)),
	Node4_VC2 = evc:increment(evc:increment(Node4_VC1)),
	Merge1 = merge(Node4_VC2, Node1_VC3),
	io:format("~90p~n", [[{node1, Node1_VC3}, {node4, Node4_VC2}]]),
	timer:sleep(500),
	Node1_VC4 = evc:increment(Node1_VC3),
	Node4_VC3 = evc:increment(Merge1),
	io:format("~90p~n", [[{node1, Node1_VC4}, {node4, Node4_VC3}]]),
	Merge2 = merge(Node1_VC4, Node4_VC3),
	timer:sleep(100),
	Node1_VC5 = evc:increment(Merge2),
	Node4_VC4 = evc:increment(Node4_VC3),
	timer:sleep(500),
	io:format("~90p~n", [[{node1, Node1_VC5}, {node4, Node4_VC4}]]).

-ifdef(TEST).
example_test() ->
	A = evc:new(1),
	B = evc:new(2),
	A1 = evc:increment(A),
	B1 = evc:increment(B),
	?assert(evc:descends(A1,A)),
	?assert(evc:descends(B1,B)),
	?assertNot(evc:descends(A1,B1)),
	A2 = evc:increment(A1),
	C = evc:merge(A2, B1),
	C1 = evc:increment(C),
	?assertEqual(2, counter(A2)),
	?assertEqual(3, counter(C1)),
	?assert(evc:descends(C1, A2)),
	?assert(evc:descends(C1, B1)),
	?assertNot(evc:descends(B1, C1)),
	?assertNot(evc:descends(B1, A1)).

-endif.