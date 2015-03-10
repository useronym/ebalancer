-module(tester).

%% API
-export([test_nodes/2, test/1, test_stream/2, test_stream_nodes/3]).


%% @doc A quick test that sends 'Length' messages into the system, each stamped
%% with an increasing global ID.
test(Length) ->
    test_nodes(Length, [node() | nodes()]).

%% Same as above, but takes Nodes to send messages to as an argument.
test_nodes(Length, Nodes) ->
    test_nodes(1, Length + 1, Nodes).
test_nodes(N, N, _Nodes) ->
    ok;
test_nodes(N, Length, Nodes) ->
    Target = lists:nth(random:uniform(length(Nodes)), Nodes),
    gen_server:cast({ebalancer_controller, Target}, {send_tcp, test, N}),
    test_nodes(N + 1, Length, Nodes).

test_stream(MsgsPerSecond, Duration) ->
    test_stream_nodes(MsgsPerSecond, Duration, [node | nodes()]).

test_stream_nodes(_, Duration, _) when Duration =< 0 ->
    ok;
test_stream_nodes(MsgsPerSecond, Duration, Nodes) ->
    {_, Secs, MicroSecs} = os:timestamp(),
    test_nodes(MsgsPerSecond, Nodes),
    {_, SecsAfter, MicroSecsAfter} = os:timestamp(),
    MillisElapsed = (SecsAfter - Secs)*1000 + (MicroSecsAfter - MicroSecs) div 1000,
    if MillisElapsed =< 1000 ->
        timer:sleep(1000 - MillisElapsed),
        test_stream_nodes(MsgsPerSecond, Duration - 1, Nodes);
       MillisElapsed > 1000 ->
        test_stream_nodes(MsgsPerSecond, Duration - MillisElapsed div 1000, Nodes)
    end.