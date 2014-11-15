-module(tcp_server_sup).
-author("danos").

-behaviour(supervisor).

%% API
-export([start_link/2, start_client/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(MAX_RESTART, 5).
-define(MAX_TIME, 60).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Port, Module) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, [Port, Module]).

%% A startup function for spawning new client connection handling FSM.
%% To be called by the TCP listener process.
start_client() ->
  supervisor:start_child(tcp_client_sup, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Port, Module]) ->
  SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME},
  ListenerMFA = {tcp_listener, start_link, [Port]},
  ListenerChild = {tcp_server_sup, ListenerMFA, permanent, 2000, worker, [tcp_listener]},
  ClientSupMFA = {supervisor, start_link, [{local, tcp_client_sup}, ?MODULE, [Module]]},
  ClientSupChild = {tcp_client_sup, ClientSupMFA, permanent, infinity, supervisor, []},
  {ok, {SupFlags, [ListenerChild, ClientSupChild]}};
init([Module]) ->
  SupFlags = {simple_one_for_one, ?MAX_RESTART, ?MAX_TIME},
  TcpClientMFA = {tcp_fsm, start_link, [Module]},
  TcpClientChild = {undefined, TcpClientMFA, temporary, 2000, worker, []},
  {ok, {SupFlags, [TcpClientChild]}}.