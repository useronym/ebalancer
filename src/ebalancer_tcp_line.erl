-module(ebalancer_tcp_line).

-behaviour(gen_tcp_server).

%% API
-export([start_link/1]).

%% gen_tcp_server callbacks
-export([handle_accept/1, handle_tcp/3, handle_close/3]).

-record(state, {name}).

%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

%% @doc Start a TCP echo server.
-spec start_link(integer()) -> ok.
start_link(Port) ->
    gen_tcp_server:start_link(?MODULE, Port, [{packet, line}]).

%%%-----------------------------------------------------------------------------
%%% gen_tcp_server_handler callbacks
%%%-----------------------------------------------------------------------------

%% @private
handle_accept(_Socket) ->
    {ok, #state{name = syslog}}.

%% @private
handle_tcp(_Socket, Data, State) ->
    ebalancer_balancer:receive_data(State#state.name, Data),
    {ok, State}.

%% @private
handle_close(_Socket, _Reason, _State) ->
    ok.
