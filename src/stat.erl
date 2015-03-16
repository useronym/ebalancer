-module(stat).

-behaviour(gen_server).

%% API
-export([start/0, start_link/0, stop/0, stat/1, get_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

start() ->
    gen_server:start({global, ?MODULE}, ?MODULE, [], []).

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call({global, ?MODULE}, stop).

%% Adds another batch into the statistics. Takes a list of tuples, where the first
%% element is ordered vector clocks and the second is ordered (global!) message IDs.
stat(Zipped) ->
    gen_server:cast({global, ?MODULE}, {stat, Zipped, node()}).

get_stats() ->
    gen_server:call({global, ?MODULE}, get_stats).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    Map = #{comparability => 1,
        accuracy => 1,
        accuracy_strict => 1,
        average_jump => 1,
        %% average_batch_miss - On average, how many entries from batch n+1
        %% should have been put before the last entry in batch n.
        %% Ideal value would be 0, however anything higher than 'average_jump' means
        %% the collection process is messing up the order more than it already is.
        average_batch_miss => 0,
        collecting_nodes => #{}},
    {ok, maps:put(entries_count, 0, Map)}.


handle_call(get_stats, _From, Map) ->
    {reply, Map, Map};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.



handle_cast({stat, [], _}, Map) ->
    {noreply, Map};
handle_cast({stat, Zipped, FromNode}, M) ->
    {VCs, IDs} = lists:unzip(Zipped),
    ThisCount = length(VCs),
    TotalCount = maps:get(entries_count, M),

    PrevComp = maps:get(comparability, M),
    M1 = maps:update(comparability, weighted_avg(comparability(VCs), ThisCount, PrevComp, TotalCount), M),

    PrevAcc = maps:get(accuracy, M),
    M2 = maps:update(accuracy, weighted_avg(accuracy(IDs), ThisCount, PrevAcc, TotalCount), M1),

    PrevAccS = maps:get(accuracy_strict, M),
    M3 = maps:update(accuracy_strict, weighted_avg(accuracy_strict(IDs), ThisCount, PrevAccS, TotalCount), M2),

    PrevJump = maps:get(average_jump, M),
    M4 = maps:update(average_jump, weighted_avg(average_jump(IDs), ThisCount, PrevJump, TotalCount), M3),

    PrevMiss = maps:get(average_batch_miss, M),
    [DefaultLastID | _] = IDs,
    LastID = maps:get(last_id, M, DefaultLastID),
    M5 = maps:update(average_batch_miss, weighted_avg(batch_miss(LastID, IDs), ThisCount, PrevMiss, TotalCount), M4),
    M51 = maps:put(last_id, lists:last(IDs), M5),


    NodesMap = maps:get(collecting_nodes, M),
    ThisNodeCount = maps:get(FromNode, NodesMap, 0) + 1,
    M6 = maps:put(collecting_nodes, maps:put(FromNode, ThisNodeCount, NodesMap), M51),
    {noreply, maps:update(entries_count, TotalCount + ThisCount, M6)}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%-----------------------------------------------------------------------------
%%% internal functions
%%%-----------------------------------------------------------------------------

%% @doc Weighted average, where A has WeightA and B has WeightB.
weighted_avg(A, WeightA, B, WeightB) ->
    (A*WeightA + B*WeightB) / (WeightA + WeightB).


%% @doc Takes an ordered list of vclocks and computes how many
%% can be/have been ordered without tne need of comparing timestamps.
comparability([]) ->
    undefined;
comparability(VCs) ->
    {Comp, _} = lists:foldl(fun(VC, {Ok, PrevVC}) ->
        case vclock:descends(VC, PrevVC) of
            true ->
                {Ok + 1, VC};
            false ->
                {Ok, VC}
        end
    end,
        {0, vclock:fresh()},
        VCs),
    Comp / length(VCs).


%% @doc Takes a list of ordered messages and computes how many are correctly ordered.
%% The messages have to be integers. The ideal value is 1.
%% By a 'good' message is meant that the previous message has a lower ID
accuracy([]) ->
    undefined;
accuracy(Msgs) ->
    {TotalGood, _} = lists:foldl(fun(Num, {Good, PrevNum}) ->
        case PrevNum < Num of
            true ->
                {Good + 1, Num};
            false ->
                {Good, Num}
        end
    end,
        {0, 0},
        Msgs),
    TotalGood / length(Msgs).


%% @doc Takes a list of ordered messages and computes how many are correctly ordered.
%% The messages have to be integers. The ideal value is 1.
%% By a 'good' message is meant that the previous message has ID of MyID - 1
accuracy_strict([]) ->
    undefined;
accuracy_strict(Msgs) ->
    {TotalGood, _} = lists:foldl(fun(Num, {Good, PrevNum}) ->
        case PrevNum + 1 == Num of
            true ->
                {Good + 1, Num};
            false ->
                {Good, Num}
        end
    end,
        {0, 0},
        Msgs),
    TotalGood / length(Msgs).


%% @doc Takes a list of ordered messages and computes the average "jump"
%% between the messages (i.e. the average jump of 1.0 would be ideal).
average_jump([]) ->
    undefined;
average_jump(Msgs) ->
    [FirstID | _] = Msgs,
    {TotalJumps, _} = lists:foldl(fun(ID, {JumpAcc, PrevID}) ->
        {JumpAcc + abs(ID - PrevID), ID}
    end,
        {0, FirstID - 1},
        Msgs),
    TotalJumps / length(Msgs).


%% @doc Computes how many entries should have been ordered before the
%% last entry from previous batch.
batch_miss(LastID, IDs) ->
    length(lists:takewhile(fun(ThisID) -> ThisID < LastID end, IDs)).