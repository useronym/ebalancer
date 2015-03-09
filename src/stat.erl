-module(stat).

-behaviour(gen_server).

%% API
-export([start_link/0, stat/1, get_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%% Adds another batch into the statistics. Takes a list of tuples, where the first
%% element is ordered vector clocks and the second is ordered (global!) message IDs.
stat(Zipped) ->
    gen_server:cast({global, ?MODULE}, {stat, Zipped}).

get_stats() ->
    gen_server:call({global, ?MODULE}, get_stats).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    Map = #{comparability => 1,
        accuracy => 1,
        accuracy_strict => 1,
        average_jump => 1},
    {ok, maps:put(entries_count, 0, Map)}.


handle_call(get_stats, _From, Map) ->
    {reply, Map, Map}.


handle_cast({stat, Zipped}, M) ->
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
    {noreply, maps:update(entries_count, TotalCount + ThisCount, M4)}.


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


%% @doc Takes an ordered list of ordered vclocks and computes how many
%% can be ordered without tne need of comparing timestamps.
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


%% @doc Takes a lists of ordered messages and computes how many are correctly ordered.
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


%% @doc Takes a lists of ordered messages and computes how many are correctly ordered.
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
    {TotalJumps, _} = lists:foldl(fun(ID, {JumpAcc, PrevID}) ->
        {JumpAcc + abs(ID - PrevID), ID}
    end,
        {0, 0},
        Msgs),
    TotalJumps / length(Msgs).