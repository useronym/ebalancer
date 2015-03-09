-module(stat).

%% API
-export([all/1, clocks/1, accuracy/1, accuracy_strict/1]).

all(List) ->
    VCs = [VC || {VC, _} <- List],
    Msgs = [Msg || {_, Msg} <- List],
    {{comparability, clocks(VCs)},
        {accuracy, accuracy(Msgs)},
        {accuracy_strict, accuracy_strict(Msgs)},
        {average_jump, average_jump(Msgs)}}.


%% @doc Takes an ordered list of ordered vclocks and computes how many
%% can be ordered without tne need of comparing timestamps.
clocks([]) ->
    undefined;
clocks(VCs) ->
    {Comp, NotComp, _} = lists:foldl(fun(VC, {Ok, Nok, PrevVC}) ->
        case vclock:descends(VC, PrevVC) of
            true ->
                {Ok + 1, Nok, VC};
            false ->
                {Ok, Nok + 1, VC}
        end
    end,
        {0, 0, vclock:fresh()},
        VCs),
    {{comparable, Comp}, {nope, NotComp}}.


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