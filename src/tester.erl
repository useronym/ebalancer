-module(tester).

%% API
-export([test_nodes/2, test/1, test_stream/2, test_stream_nodes/3]).


%% @doc A quick test that sends 'Length' messages into the system, each stamped
%% with an increasing global ID.
test(Length) ->
    test_nodes(Length, [node() | nodes()]).

%% Same as above, but takes Nodes to send messages to as an argument.
test_nodes(Length, Nodes) ->
    test_nodes(Length, Nodes, 1).
%% This variant starts at the given id
test_nodes(Length, Nodes, StartID) ->
    test_nodes1(StartID, Length + StartID, Nodes).

test_nodes1(N, N, _Nodes) ->
    ok;
test_nodes1(N, Length, Nodes) ->
    Target = lists:nth(random:uniform(length(Nodes)), Nodes),
    gen_server:cast({ebalancer_controller, Target}, {send_tcp, test, N}),
    test_nodes1(N + 1, Length, Nodes).


test_stream(MsgsPerSecond, Duration) ->
    test_stream_nodes(MsgsPerSecond, Duration, [node() | nodes()]).

test_stream_nodes(MsgsPerSecond, Duration, Nodes) ->
    test_stream_nodes1(MsgsPerSecond, Duration, Nodes, 1).

test_stream_nodes1(_, Duration, _, _) when Duration =< 0 ->
    ok;
test_stream_nodes1(MsgsPerSecond, Duration, Nodes, StartID) ->
    {_, Secs, MicroSecs} = os:timestamp(),
    test_nodes(MsgsPerSecond, Nodes, StartID),
    {_, SecsAfter, MicroSecsAfter} = os:timestamp(),
    MillisElapsed = round((SecsAfter - Secs)*1000 + (MicroSecsAfter - MicroSecs)/1000),
    if MillisElapsed =< 1000 ->
        timer:sleep(1000 - MillisElapsed),
        test_stream_nodes1(MsgsPerSecond, Duration - 1, Nodes, StartID + MsgsPerSecond);
       MillisElapsed > 1000 ->
        SecsElapsed = round(MillisElapsed/1000),
        io:format("Can't keep up! Sent ~p messages in ~p seconds", [MsgsPerSecond, SecsElapsed]),
        test_stream_nodes1(MsgsPerSecond, Duration - SecsElapsed, Nodes, StartID + MsgsPerSecond)
    end.