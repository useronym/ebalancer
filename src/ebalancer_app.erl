-module(ebalancer_app).
-author("osense").

-behaviour(application).

%% Application callbacks
-export([start/2, start/1, stop/1]).


%%%-----------------------------------------------------------------------------
%%% Application callbacks
%%%-----------------------------------------------------------------------------

start(_StartType, _StartArgs)->
    start(balancer).

start([balancer]) ->
    ebalancer_balancer:start_link();
start([worker]) ->
    {ok, Host} = inet:gethostname(),
    true = net_kernel:connect_node(list_to_atom("ebalancer@" ++ Host)),
    timer:sleep(5000),
    ebalancer_worker:start_link().

stop(_State) ->
    ok.
