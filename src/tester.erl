-module(tester).

%% API
-export([test_nodes/2, test/1]).


%% Reads a file and distributed it to Nodes, breaking it on line ends.
test_nodes(Filename, Nodes) ->
    {ok, Binary} = file:read_file(Filename),
    Lines = binary:split(Binary, <<$\n>>, [global]),
    lists:foreach(fun(Line) ->
        Target = lists:nth(random:uniform(length(Nodes)), Nodes),
        gen_server:cast({ebalancer_controller, Target}, {send_tcp, test, Line})
        end,
    Lines).

%% A quick test that send Length messages into the system.
test(Length) when is_integer(Length) ->
    test(1, Length + 1).
test(N, N) ->
    ok;
test(N, Length) ->
    Nodes = [node() | nodes()],
    Target = lists:nth(random:uniform(length(Nodes)), Nodes),
    gen_server:cast({ebalancer_controller, Target}, {send_tcp, test, N}),
    timer:sleep(1),
    test(N + 1, Length).