%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%% 2015 A. Krupicka
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc A simple Erlang implementation of vector clocks as inspired by Lamport logical clocks.
%%
%% @reference Leslie Lamport (1978). "Time, clocks, and the ordering of events
%% in a distributed system". Communications of the ACM 21 (7): 558-565.
%%
%% @reference Friedemann Mattern (1988). "Virtual Time and Global States of
%% Distributed Systems". Workshop on Parallel and Distributed Algorithms:
%% pp. 215-226

-module(vclock).

-export([
    fresh/0,
    descends/2,
    dominates/2,
    merge2/2,
    merge/1,
    get_counter/2,
    get_dot/2,
    valid_dot/1,
    increment/2,
    increment/3,
    all_nodes/1,
    equal/2,
    prune/3,
    timestamp/0,
    increment/1,
    compare/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([vclock/0, timestamp/0, vclock_node/0, pure_dot/0]).

-type vclock() :: {[pure_dot()], timestamp()}.
-type pure_dot() :: {vclock_node(), counter()}.

% Nodes can have any term() as a name, but they must differ from each other.
-type vclock_node() :: term().
-type counter() :: integer().
-type timestamp() :: {integer(), integer(), integer()}.

% @doc Create a brand new vclock.
-spec fresh() -> vclock().
fresh() ->
    {[], {0, 0, 0}}.

% @doc Return true if Va is a direct descendant of Vb, else false -- remember, a vclock is its own descendant!
-spec descends(Va :: vclock(), Vb :: vclock()) -> boolean().
descends(_, {[], _}) ->
    % all vclocks descend from the empty vclock
    true;
descends({Va, Ta}, {Vb, Tb}) ->
    [{NodeB, CtrB}|RestB] = Vb,
    case lists:keyfind(NodeB, 1, Va) of
        false ->
            false;
        {_, CtrA} ->
            (CtrA >= CtrB) andalso descends({Va, Ta}, {RestB, Tb})
    end.

%% @doc true if `A' strictly dominates `B'. Note: ignores
%% timestamps. In Riak it is possible to have vclocks that are
%% identical except for timestamps. When two vclocks descend each
%% other, but are not equal, they are concurrent. See source comment
%% for more details. (Actually you can have indentical clocks
%% including timestamps, that represent different events, but let's
%% not go there.)
%%
-spec dominates(vclock(), vclock()) -> boolean().
dominates(A, B) ->
    %% In a sane world if two vclocks descend each other they MUST be
    %% equal. In riak they can descend each other and have different
    %% timestamps(!) How? Deleted keys, re-written, then restored is
    %% one example. See riak_kv#679 for others. This is why we must
    %% check descends both ways rather than checking descends(A, B)
    %% and not equal(A, B). Do not "optimise" this to dodge the second
    %% descends call! I know that the laws of causality say that each
    %% actor must act serially, but Riak breaks that.
    descends(A, B) andalso not descends(B, A).

% @doc Merge 2 VClocks, recalculating the mean timestamp.
-spec merge2(Va :: vclock(), Vb :: vclock()) -> vclock().
merge2({[], A}, {[], _}) ->
    {[], A};
merge2({Va, {Ta1, Ta2, Ta3}}, {Vb, {Tb1, Tb2, Tb3}}) ->
    Wa = length(Va),
    Wb = length(Vb),
    W = Wa + Wb,
    {merge([Va, Vb]), {(Ta1*Wa + Tb1*Wb) div W,
        (Ta2*Wa + Tb2*Wb) div W,
        (Ta3*Wa + Tb3*Wb) div W}}.

% @doc Combine all VClocks in the input list into their least possible
%      common descendant.
-spec merge(VClocks :: [[pure_dot()]]) -> [pure_dot()].
merge([])             -> [];
merge([SingleVclock]) -> SingleVclock;
merge([First|Rest])   -> merge(Rest, lists:keysort(1, First)).

merge([], NClock) -> NClock;
merge([AClock|VClocks],NClock) ->
    merge(VClocks, merge(lists:keysort(1, AClock), NClock, [])).

merge([], [], AccClock) -> lists:reverse(AccClock);
merge([], Left, AccClock) -> lists:reverse(AccClock, Left);
merge(Left, [], AccClock) -> lists:reverse(AccClock, Left);
merge(V=[{Node1, Ctr1} = NCT1 | VClock],
    N=[{Node2, Ctr2} = NCT2 | NClock], AccClock) ->
    if Node1 < Node2 ->
        merge(VClock, N, [NCT1|AccClock]);
       Node1 > Node2 ->
            merge(V, NClock, [NCT2|AccClock]);
       true ->
           CT = if Ctr1 >= Ctr2 -> Ctr1;
                   Ctr1 < Ctr2 -> Ctr2
                end,
           merge(VClock, NClock, [{Node1,CT}|AccClock])
    end.

% @doc Get the counter value in VClock set from Node.
-spec get_counter(Node :: vclock_node(), VClock :: vclock()) -> counter().
get_counter(Node, {VClock, _}) ->
    case lists:keyfind(Node, 1, VClock) of
        {_, Ctr} -> Ctr;
        false           -> 0
    end.

% @doc Get the entry `dot()' for `vclock_node()' from `vclock()'.
-spec get_dot(Node :: vclock_node(), VClock :: vclock()) -> {ok, pure_dot()} | undefined.
get_dot(Node, {VClock, _}) ->
    case lists:keyfind(Node, 1, VClock) of
        false -> undefined;
        Entry -> {ok, Entry}
    end.

%% @doc is the given argument a valid dot, or entry?
-spec valid_dot(pure_dot()) -> boolean().
valid_dot({_, Cnt}) when is_integer(Cnt) ->
    true;
valid_dot(_) ->
    false.

% @doc Increment VClock at Node.
-spec increment(Node :: vclock_node(), VClock :: vclock()) -> vclock().
increment(Node, VClock) ->
    increment(Node, timestamp(), VClock).

% @doc Increment VClock at Node.
-spec increment(Node :: vclock_node(), IncTs :: timestamp(),
    VClock :: vclock()) -> vclock().
increment(Node, IncTs, {VClock, Ts = {TsM, TsS, TsU}}) ->
    {C1, NewV} = case lists:keytake(Node, 1, VClock) of
                     false ->
                         {1, VClock};
                     {value, {_N, C}, ModV} ->
                         {C + 1, ModV}
                 end,
    {DM, DS, DU} = tdiff(IncTs, Ts),
    W = length(NewV) + 1,
    NewTs = {TsM + DM div W, TsS + DS div W, TsU+ DU div W},
    {[{Node,C1}|NewV], NewTs}.


% @doc Return the list of all nodes that have ever incremented VClock.
-spec all_nodes(VClock :: vclock()) -> [vclock_node()].
all_nodes({VClock, _}) ->
    [X || {X, _} <- VClock].

% @doc Return a timestamp for a vector clock
-spec timestamp() -> timestamp().
timestamp() ->
    os:timestamp().

-spec tdiff(timestamp(), timestamp()) -> timestamp().
tdiff({Am, As, Au}, {Bm, Bs, Bu}) ->
    {Am-Bm, As-Bs, Au-Bu}.

% @doc Compares two VClocks for equality.
-spec equal(VClockA :: vclock(), VClockB :: vclock()) -> boolean().
equal({VA, _}, {VB, _}) ->
    lists:sort(VA) =:= lists:sort(VB).

% @doc Possibly shrink the size of a vclock, depending on current age and size.
-spec prune(V::vclock(), Now::integer(), BucketProps::term()) -> vclock().
prune(V,Now,BucketProps) ->
    %% This sort need to be deterministic, to avoid spurious merge conflicts later.
    %% We achieve this by using the node ID as secondary key.
    SortV = lists:sort(fun({N1,{_,T1}},{N2,{_,T2}}) -> {T1,N1} < {T2,N2} end, V),
    prune_vclock1(SortV,Now,BucketProps).
% @private
prune_vclock1(V,Now,BProps) ->
    case length(V) =< get_property(small_vclock, BProps) of
        true -> V;
        false ->
            {_,{_,HeadTime}} = hd(V),
            case (Now - HeadTime) < get_property(young_vclock,BProps) of
                true -> V;
                false -> prune_vclock1(V,Now,BProps,HeadTime)
            end
    end.
% @private
prune_vclock1(V,Now,BProps,HeadTime) ->
    % has a precondition that V is longer than small and older than young
    case (length(V) > get_property(big_vclock,BProps)) orelse
        ((Now - HeadTime) > get_property(old_vclock,BProps)) of
        true -> prune_vclock1(tl(V),Now,BProps);
        false -> V
    end.

get_property(Key, PairList) ->
    case lists:keyfind(Key, 1, PairList) of
        {_Key, Value} ->
            Value;
        false ->
            undefined
    end.

% @doc Increment VClock at current node.
-spec increment(VClock :: vclock()) -> vclock().
increment(VClock) ->
    increment(node(), timestamp(), VClock).

% @doc Get the name of the most fresh value in a VClock.
-spec get_oldest_node(VClock :: vclock()) -> {vclock_node()}.
get_oldest_node(VClock) ->
    Pairs = ([{Node, get_counter(Node, VClock)} || Node <- all_nodes(VClock)]),
    {Oldest, _} = lists:foldl(
        fun({Node, Count}, {_, MaxCount}) when Count >= MaxCount ->
                {Node, Count};
            ({_, Count}, {MaxNode, MaxCount}) when Count < MaxCount ->
                {MaxNode, MaxCount}
        end,
        {'', 0},
        Pairs),
    Oldest.


%% @doc Get the mean timestamp of a vector clock.
-spec get_mean_timestamp(VClock :: vclock()) -> timestamp().
get_mean_timestamp({_VClock, Ts}) ->
    Ts.

% @doc Returns true if Va is less than or equal to Vb, else false
compare(Va, Vb) ->
    case descends(Vb, Va) of
        true -> true;
        false ->
            case descends(Va, Vb) of
                true -> false;
                false ->
                    Ta = get_mean_timestamp(Va),
                    Tb = get_mean_timestamp(Vb),
                    if Ta < Tb -> true;
                       Ta > Tb -> false;
                       Ta == Tb ->
                           get_oldest_node(Va) < get_oldest_node(Vb)
                    end
            end
    end.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

mean_timestamp_test() ->
    A = vclock:fresh(),
    A1 = vclock:increment(a, A),
    timer:sleep(50),
    A2 = vclock:increment(b, A1),
    T = now(),
    timer:sleep(50),
    A3 = vclock:increment(c, A2),
    ?assertMatch(X when abs(X) < 1000, timer:now_diff(get_mean_timestamp(A3), T)).

% To avoid an unnecessary dependency, we paste a function definition from riak_core_until.
riak_core_until_moment() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

% doc Serves as both a trivial test and some example code.
example_test() ->
    A = vclock:fresh(),
    B = vclock:fresh(),
    A1 = vclock:increment(a, A),
    B1 = vclock:increment(b, B),
    true = vclock:descends(A1,A),
    true = vclock:descends(B1,B),
    false = vclock:descends(A1,B1),
    A2 = vclock:increment(a, A1),
    C = vclock:merge2(A2, B1),
    C1 = vclock:increment(c, C),
    true = vclock:descends(C1, A2),
    true = vclock:descends(C1, B1),
    false = vclock:descends(B1, C1),
    false = vclock:descends(B1, A1),
    ok.

prune_small_test() ->
    % vclock with less entries than small_vclock will be untouched
    Now = riak_core_until_moment(),
    OldTime = Now - 32000000,
    SmallVC = [{<<"1">>, {1, OldTime}},
        {<<"2">>, {2, OldTime}},
        {<<"3">>, {3, OldTime}}],
    Props = [{small_vclock,4}],
    ?assertEqual(lists:sort(SmallVC), lists:sort(prune(SmallVC, Now, Props))).

prune_young_test() ->
    % vclock with all entries younger than young_vclock will be untouched
    Now = riak_core_until_moment(),
    NewTime = Now - 1,
    VC = [{<<"1">>, {1, NewTime}},
        {<<"2">>, {2, NewTime}},
        {<<"3">>, {3, NewTime}}],
    Props = [{small_vclock,1},{young_vclock,1000}],
    ?assertEqual(lists:sort(VC), lists:sort(prune(VC, Now, Props))).

prune_big_test() ->
    % vclock not preserved by small or young will be pruned down to
    % no larger than big_vclock entries
    Now = riak_core_until_moment(),
    NewTime = Now - 1000,
    VC = [{<<"1">>, {1, NewTime}},
        {<<"2">>, {2, NewTime}},
        {<<"3">>, {3, NewTime}}],
    Props = [{small_vclock,1},{young_vclock,1},
        {big_vclock,2},{old_vclock,100000}],
    ?assert(length(prune(VC, Now, Props)) =:= 2).

prune_old_test() ->
    % vclock not preserved by small or young will be pruned down to
    % no larger than big_vclock and no entries more than old_vclock ago
    Now = riak_core_until_moment(),
    NewTime = Now - 1000,
    OldTime = Now - 100000,
    VC = [{<<"1">>, {1, NewTime}},
        {<<"2">>, {2, OldTime}},
        {<<"3">>, {3, OldTime}}],
    Props = [{small_vclock,1},{young_vclock,1},
        {big_vclock,2},{old_vclock,10000}],
    ?assert(length(prune(VC, Now, Props)) =:= 1).

prune_order_test() ->
    % vclock with two nodes of the same timestamp will be pruned down
    % to the same node
    Now = riak_core_until_moment(),
    OldTime = Now - 100000,
    VC1 = [{<<"1">>, {1, OldTime}},
        {<<"2">>, {2, OldTime}}],
    VC2 = lists:reverse(VC1),
    Props = [{small_vclock,1},{young_vclock,1},
        {big_vclock,2},{old_vclock,10000}],
    ?assertEqual(prune(VC1, Now, Props), prune(VC2, Now, Props)).

accessor_test() ->
    VC = {[{<<"1">>, 1},
        {<<"2">>, 2}], now()},
    ?assertEqual(1, get_counter(<<"1">>, VC)),
    ?assertEqual(2, get_counter(<<"2">>, VC)),
    ?assertEqual(0, get_counter(<<"3">>, VC)),
    ?assertEqual([<<"1">>, <<"2">>], all_nodes(VC)).

merge2_test() ->
    VC1 = increment(1, increment(1, vclock:fresh())),
    VC2 = increment(2, vclock:fresh()),
    {A1, B1, C1} = get_mean_timestamp(VC1),
    {A2, B2, C2} = get_mean_timestamp(VC2),
    ?assertEqual({[], {0, 0, 0}}, merge2(vclock:fresh(), vclock:fresh())),
    ?assertEqual({[{1, 2}, {2, 1}],
        {(A1+A2) div 2, (B1+B2) div 2, (C1+C2) div 2}},
        merge2(VC1, VC2)).

merge_less_left_test() ->
    VC1 = [{<<"5">>, {5, 5}}],
    VC2 = [{<<"6">>, {6, 6}}, {<<"7">>, {7, 7}}],
    ?assertEqual([{<<"5">>, {5, 5}},{<<"6">>, {6, 6}}, {<<"7">>, {7, 7}}],
        vclock:merge([VC1, VC2])).

merge_less_right_test() ->
    VC1 = [{<<"6">>, {6, 6}}, {<<"7">>, {7, 7}}],
    VC2 = [{<<"5">>, {5, 5}}],
    ?assertEqual([{<<"5">>, {5, 5}},{<<"6">>, {6, 6}}, {<<"7">>, {7, 7}}],
        vclock:merge([VC1, VC2])).

merge_same_id_test() ->
    VC1 = [{<<"1">>, {1, 2}},{<<"2">>,{1,4}}],
    VC2 = [{<<"1">>, {1, 3}},{<<"3">>,{1,5}}],
    ?assertEqual([{<<"1">>, {1, 3}},{<<"2">>,{1,4}},{<<"3">>,{1,5}}],
        vclock:merge([VC1, VC2])).

get_entry_test() ->
    VC = vclock:fresh(),
    VC1 = increment(a, increment(c, increment(b, increment(a, VC)))),
    ?assertMatch({ok, {a, 2}}, get_dot(a, VC1)),
    ?assertMatch({ok, {b, 1}}, get_dot(b, VC1)),
    ?assertMatch({ok, {c, 1}}, get_dot(c, VC1)),
    ?assertEqual(undefined, get_dot(d, VC1)).

valid_entry_test() ->
    VC = vclock:fresh(),
    VC1 = increment(c, increment(b, increment(a, VC))),
    [begin
         {ok, E} = get_dot(Actor, VC1),
         ?assert(valid_dot(E))
     end || Actor <- [a, b, c]],
    ?assertNot(valid_dot(undefined)),
    ?assertNot(valid_dot("huffle-puff")),
    ?assertNot(valid_dot([])).

-endif.
