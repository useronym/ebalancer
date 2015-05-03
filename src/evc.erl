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
-export([perf1/0, new/1, increment/1, node_id/1, counter/1, counter/2, merge/2, merge/3, compare/2]).

-type timestamp() :: integer().
-type evc() :: {list(), timestamp(), {atom(), timestamp()}}.

-spec new(atom()) -> evc().
new(Node) ->
  new(Node, timestamp()).

new(Node, NodeTime) ->
  EmptyVCList = lists:keystore(Node, 1, [], {Node, 0}),
  {EmptyVCList, 0, {Node, NodeTime}}.

-spec increment(evc()) -> evc().
increment(VC) ->
  increment(timestamp(), VC).

increment(NodeTime, {VCList, TA, {Node, LastNodeTime}}) ->
  TimeShift = NodeTime - LastNodeTime,
  NewVCList = lists:keyreplace(Node, 1, VCList, {Node, counter(Node, VCList) + 1}),
  {NewVCList, TA + TimeShift, {Node, NodeTime}}.

-spec node_id(evc()) -> atom().
node_id({_, _, {Node, _}}) ->
  Node.

-spec counter(evc()) -> integer().
counter({VCList, _, {Node, _}}) ->
  counter(Node, VCList).

-spec counter(atom(), list()) -> integer().
counter(Node, VCList) ->
  {Node, Counter} = lists:keyfind(Node, 1, VCList),
  Counter.

-spec merge(evc(), evc()) -> evc().
merge(VC1, VC2) ->
  merge(timestamp(), VC1, VC2, 0).

-spec merge(evc(), evc(), integer()) -> evc().
merge(VC1, VC2, RTTDelta) ->
  merge(timestamp(), VC1, VC2, RTTDelta).

merge(NodeTime, {LocalVCList, LocalTA, {Node, LastNodeTime}}, {RemoteVCList, RemoteTA, _}, RTTDelta) ->
  TimeShift = NodeTime - LastNodeTime,
  VCListMerge = vcl_merge(LocalVCList, RemoteVCList),
  TAAproximation = approximate_ta(LocalTA, RemoteTA, TimeShift, RTTDelta),
  {VCListMerge, TAAproximation, {Node, NodeTime}}.

vcl_merge(VCList1, VCList2) ->
  lists:reverse(keymerge(VCList1, VCList2, [], fun max/2)).

%% merges lists by keys, calls Fun when the keys are equal, inspired by lists:keymerge/4
keymerge([], [], M, _Fun) ->
  M;
keymerge([], Rest, M, _Fun) ->
  lists:reverse(Rest, M);
keymerge(Rest, [], M, _Fun) ->
  lists:reverse(Rest, M);
keymerge([A = {K1, _} | T1], L2 = [{K2, _} | _T2], M, Fun) when K1 < K2 ->
  keymerge(T1, L2, [A | M], Fun);
keymerge(L1 = [{K1, _} | _T1], [B = {K2, _} | T2], M, Fun) when K1 > K2 ->
  keymerge(T2, L1, [B | M], Fun);
keymerge([{K, V1} | T1], [{K, V2} | T2], M, Fun) -> %% when keys are equal
  keymerge(T1, T2, [{K, Fun(V1, V2)} | M], Fun).

approximate_ta(LocalTA, RemoteTA, TimeShift, RTTDelta) ->
  ((LocalTA + TimeShift) + (RemoteTA + RTTDelta)) div 2.

-spec compare(evc(), evc()) -> boolean().
compare({VCList1, TA1, {Node1, _}}, {VCList2, TA2, {Node2, _}}) ->
  case vcl_lte(VCList1, VCList2) of
    true -> true; %% the VCList1 is lower than or equal VCList2 -> so is the whole EVC
    false -> %% VCList1 can be either greater than VCList2, or concurrent
      case vcl_lte(VCList2, VCList1) of
        true -> false; %% the VCList1 is greater than VCList2 -> so is the whole EVC
        false -> {TA1, Node1} =< {TA2, Node2} %% The VCLists are concurrent -> TIEBREAK: Timestamp Approx. and then Node Name
      end
  end.

vcl_lte([], _) -> %% all the entries in VCList1 were matched
  true;
vcl_lte(_, []) -> %% the first entry in VCList1 does not have counter-part in VCList2
  false;
vcl_lte(VCList1 = [{Node1, _} | _], [{Node2, _} | T2]) when Node1 > Node2 ->
  vcl_lte(VCList1, T2);
vcl_lte([{Node1, _} | _], [{Node2, _} | _]) when Node1 < Node2 ->
  false;
vcl_lte([{_, Counter1} | _], [{_, Counter2} | _]) when Counter1 > Counter2 ->
  false;
vcl_lte([{_, _} | T1], [{_, _} | T2]) -> %% the patterns above did not match -> i.e Node1 == Node2 andalso Counter1 =< Counter2
  vcl_lte(T1, T2).

%% ===================================================================
%% Private functions
%% ===================================================================

timestamp() ->
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.

%% ===================================================================
%% Temporary testing functions
%% ===================================================================

perf1() ->
  Node1_VC1 = evc:new(node1),
  timer:sleep(500),
  Node1_VC2 = {_, TA1, _} = evc:increment(Node1_VC1),
  Node4New = new(node4),
  Node4_VC1 = merge(Node4New, Node1_VC2, TA1),
  timer:sleep(500),
  Node1_VC3 = evc:increment(evc:increment(Node1_VC2)),
  Node4_VC2 = evc:increment(evc:increment(Node4_VC1)),
  Merge1 = merge(Node4_VC2, Node1_VC3),
  io:format("~140p~n", [[{Node1_VC3}, {Node4_VC2}]]),
  timer:sleep(500),
  Node1_VC4 = evc:increment(Node1_VC3),
  Node4_VC3 = evc:increment(Merge1),
  io:format("~140p~n", [[{Node1_VC4}, {Node4_VC3}]]),
  Merge2 = merge(Node1_VC4, Node4_VC3),
  timer:sleep(100),
  Node1_VC5 = evc:increment(Merge2),
  Node4_VC4 = evc:increment(Node4_VC3),
  timer:sleep(500),
  io:format("~140p~n", [[{Node1_VC5}, {Node4_VC4}]]).

%% ===================================================================
%% Tests
%% ===================================================================

keymerge_test() ->
  Result = keymerge([{a, 1}, {b, 2}, {d, 4}, {e, 5}], [{b, 4}, {c, 3}, {d, 4}, {e, 6}, {g, 17}], [], fun max/2),
  ?assertEqual([{a, 1}, {b, 4}, {c, 3}, {d, 4}, {e, 6}, {g, 17}], lists:reverse(Result)).

vcl_lte_test() ->
  ?assertNot(vcl_lte([{a, 1}, {b, 2}], [{a, 2}, {b, 1}])),
  ?assert(vcl_lte([{a, 1}, {b, 1}], [{a, 2}, {b, 1}])),
  ?assert(vcl_lte([{a, 1}, {b, 1}], [{a, 1}, {b, 1}])),
  ?assertNot(vcl_lte([{a, 5}, {b, 1}], [{b, 1}])),
  ?assert(vcl_lte([{b, 1}], [{a, 5}, {b, 1}])).

compare_test() ->
  A = evc:new(node1),
  B = evc:new(node2),
  A1 = evc:increment(A),
  B1 = evc:increment(B),
  ?assert(evc:compare(A, A1)),
  ?assert(evc:compare(B, B1)),
  ?assert(evc:compare(A1, B1)),
  A2 = evc:increment(A1),
  C = evc:merge(A2, B1),
  C1 = evc:increment(C),
  ?assertEqual(2, counter(A2)),
  ?assertEqual(3, counter(C1)),
  ?assert(evc:compare(A2, C1)),
  ?assert(evc:compare(B1, C1)),
  ?assertNot(evc:compare(C1, B1)),
  ?assertNot(evc:compare(B1, A1)).

simple_test() ->
  VC1 = evc:increment(evc:new(1)),
  ?assertEqual(1, evc:counter(VC1)),
  VC2 = evc:increment(VC1),
  ?assertEqual(2, counter(VC2)).

vcl_merge_test() ->
  VC1 = [{node1, 1}, {node2, 2}, {node4, 4}],
  VC2 = [{node3, 3}, {node4, 3}],
  ?assertEqual([{node1, 1}, {node2, 2}, {node3, 3}, {node4, 4}], vcl_merge(VC1, VC2)).

vcl_merge_less_left_test() ->
  VC1 = [{node5, 5}],
  VC2 = [{node6, 6}, {node7, 7}],
  ?assertEqual([{node5, 5}, {node6, 6}, {node7, 7}], vcl_merge(VC1, VC2)).

vcl_merge_less_right_test() ->
  VC1 = [{node6, 6}, {node7, 7}],
  VC2 = [{node5, 5}],
  ?assertEqual([{node5, 5}, {node6, 6}, {node7, 7}], vcl_merge(VC1, VC2)).

vcl_merge_same_id_test() ->
  VC1 = [{node1, 1}, {node2, 1}],
  VC2 = [{node1, 1}, {node3, 1}],
  ?assertEqual([{node1, 1}, {node2, 1}, {node3, 1}], vcl_merge(VC1, VC2)).