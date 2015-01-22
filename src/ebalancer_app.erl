-module(ebalancer_app).
-author("osense").

-behaviour(application).

%% Application callbacks
-export([start/2, start/1, stop/1]).


%%%-----------------------------------------------------------------------------
%%% API functions (currently used from the start.sh script)
%%%-----------------------------------------------------------------------------

start([balancer]) ->
    application:start(ebalancer),
    timer:sleep(1000),
    ebalancer_sup:start_balancer();
start([worker]) ->
    application:start(ebalancer),
    {ok, Host} = inet:gethostname(),
    true = net_kernel:connect_node(list_to_atom("ebalancer@" ++ Host)),
    timer:sleep(5000),
    ebalancer_sup:start_worker().


%%%-----------------------------------------------------------------------------
%%% Application callbacks
%%%-----------------------------------------------------------------------------

start(_StartType, _StartArgs)->
    ebalancer_sup:start_link().

stop(_State) ->
    ok.
