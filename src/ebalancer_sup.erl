-module(ebalancer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_balancer/0, start_worker/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, infinity, Type, [I]}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_balancer() ->
    supervisor:start_child(?MODULE, ?CHILD(ebalancer_balancer_sup, supervisor)).

start_worker() ->
    supervisor:start_child(?MODULE, ?CHILD(ebalancer_worker_sup, supervisor)).

%%%-----------------------------------------------------------------------------
%%% supervisor callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.
