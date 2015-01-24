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
    ebalancer_sup:start_balancer();
start([worker]) ->
    application:start(ebalancer),
    lists:foreach(fun net_kernel:connect_node/1, net_adm:world()),
    ok = global:sync(),
    ebalancer_sup:start_worker().


%%%-----------------------------------------------------------------------------
%%% Application callbacks
%%%-----------------------------------------------------------------------------

start(_StartType, _StartArgs)->
    ebalancer_sup:start_link().

stop(_State) ->
    ok.
