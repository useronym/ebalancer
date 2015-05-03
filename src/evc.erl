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

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_SIZE, 4).

%% API
-export([perf1/0, new/1, increment/1, node_id/1, counter/1, counter/2, merge/2, merge/3, compare/2, descends/2]).

-type timestamp() :: integer().
-type evc() :: {list(), timestamp(), integer()}.

-spec new(1..99) -> evc().
new(NodeId) ->
  new(NodeId, timestamp()).

new(NodeId, NodeTime) ->
  {lists:keystore(NodeId, 1, [], {NodeId, {0, NodeTime}}), 0, NodeId}.

-spec increment(evc()) -> evc().
increment(VC) ->
  increment(timestamp(), VC).

increment(NodeTime, {VCList, TA, NodeId}) ->
  {NodeId, {Counter, LastNodeTime}} = lists:keyfind(NodeId, 1, VCList),
  TimeShift = NodeTime - LastNodeTime,
  {lists:keyreplace(NodeId, 1, VCList, {NodeId, {Counter + 1, NodeTime}}), TA + TimeShift, NodeId}.

-spec node_id(evc()) -> integer().
node_id({_, _, NodeId}) ->
  NodeId.

-spec counter(evc()) -> integer().
counter(VC = {_, _, NodeId}) ->
  counter(NodeId, VC).

-spec counter(integer(), evc()) -> integer().
counter(NodeId, {VCList, _, _}) ->
  {_, {Counter, _}} = lists:keyfind(NodeId, 1, VCList),
  Counter.

-spec merge(evc(), evc()) -> evc().
merge(VC1, VC2) ->
  merge(timestamp(), VC1, VC2, 0).

-spec merge(evc(), evc(), integer()) -> evc().
merge(VC1, VC2, RTTDelta) ->
  merge(timestamp(), VC1, VC2, RTTDelta).

merge(NodeTime, {LocalVCList, LocalTA, NodeId}, {RemoteVCList, RemoteTA, _}, RTTDelta) ->
  {NodeId, {Counter, LastNodeTime}} = lists:keyfind(NodeId, 1, LocalVCList),
  TimeShift = NodeTime - LastNodeTime,
  UpdatedLocalVCList = lists:keyreplace(NodeId, 1, LocalVCList, {NodeId, {Counter, NodeTime}}),
  VCListMerge = vclist_merge(UpdatedLocalVCList, RemoteVCList),
  TAAproximation = approximate_ta(LocalTA, RemoteTA, TimeShift, RTTDelta),
  {VCListMerge, TAAproximation, NodeId}.

vclist_merge(VCList1, VCList2) ->
  lists:reverse(keymerge(VCList1, VCList2, [], fun max/2)).

keymerge([], [], M, _Fun) -> M;
keymerge([], Rest, M, _Fun) -> lists:reverse(Rest, M);
keymerge(Rest, [], M, _Fun) -> lists:reverse(Rest, M);
keymerge([A = {K1, _} | T1], L2 = [{K2, _} | _T2], M, Fun) when K1 < K2 ->
  keymerge(T1, L2, [A | M], Fun);
keymerge(L1 = [{K1, _} | _T1], [B = {K2, _} | T2], M, Fun) when K1 > K2 ->
  keymerge(T2, L1, [B | M], Fun);
keymerge([{K, V1} | T1], [{K, V2} | T2], M, Fun) -> %% when keys are equal
  keymerge(T1, T2, [{K, Fun(V1, V2)} | M], Fun).

approximate_ta(LocalTA, RemoteTA, TimeShift, RTTDelta) ->
  ((LocalTA + TimeShift) + (RemoteTA + RTTDelta)) div 2.

-spec descends(evc(), evc()) -> boolean().
descends({VCList1, _, _}, {VCList2, _, _}) ->
  descends_2(VCList1, VCList2).

descends_2(_, []) ->
  true;
descends_2(VCList1, [{Node2, {Counter2, _}} | T2]) ->
  case lists:keyfind(Node2, 1, VCList1) of
    false ->
      false;
    {Node2, {Counter1, _}} ->
      (Counter1 >= Counter2) andalso descends_2(VCList1, T2)
  end.

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

%% ===================================================================
%% Tests
%% ===================================================================

perf1() ->
  Node1_VC1 = evc:new(node1),
  timer:sleep(500),
  Node1_VC2 = {_, TA1, _} = evc:increment(Node1_VC1),
  Node4_VC1 = merge(timestamp(), new(node4, timestamp()), Node1_VC2, TA1),
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

vclist_merge_test() ->
  Result = vclist_merge([{a, 1}, {b, 2}, {d, 4}, {e, 5}], [{b, 4}, {c, 3}, {d, 4}, {e, 6}, {g, 17}]),
  ?assertEqual([{a, 1}, {b, 4}, {c, 3}, {d, 4}, {e, 6}, {g, 17}], Result).

example_test() ->
  A = evc:new(node1),
  B = evc:new(node2),
  A1 = evc:increment(A),
  B1 = evc:increment(B),
  ?assert(evc:descends(A1, A)),
  ?assert(evc:descends(B1, B)),
  ?assertNot(evc:descends(A1, B1)),
  A2 = evc:increment(A1),
  C = evc:merge(A2, B1),
  C1 = evc:increment(C),
  ?assertEqual(2, counter(A2)),
  ?assertEqual(3, counter(C1)),
  ?assert(evc:descends(C1, A2)),
  ?assert(evc:descends(C1, B1)),
  ?assertNot(evc:descends(B1, C1)),
  ?assertNot(evc:descends(B1, A1)).

simple_test() ->
  VC1 = evc:increment(evc:new(1)),
  ?assertEqual(1, evc:counter(VC1)),
  VC2 = evc:increment(VC1),
  ?assertEqual(2, counter(VC2)).