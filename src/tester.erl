-module(tester).

%% API
-export([test_nodes/2, test/1]).


%% @doc A quick test that send 'Length' messages into the system, each stamped
%% with an increasing global ID.
test(Length) ->
    test_nodes(Length, [node() | nodes()]).

%% Same as above, but takes Nodes to send messages to as an argument.
test_nodes(Length, Nodes) when is_integer(Length) ->
    test_nodes(1, Length + 1, Nodes).
test_nodes(N, N, _Nodes) ->
    ok;
test_nodes(N, Length, Nodes) ->
    Target = lists:nth(random:uniform(length(Nodes)), Nodes),
    gen_server:cast({ebalancer_controller, Target}, {send_tcp, test, N}),
    timer:sleep(1),
    test_nodes(N + 1, Length, Nodes).