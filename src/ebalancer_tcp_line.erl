-module(ebalancer_tcp_line).

-behaviour(gen_tcp_server).

%% API
-export([start_link/2]).

%% gen_tcp_server callbacks
-export([handle_accept/2, handle_tcp/3, handle_close/3]).

-record(state, {name}).

%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

%% @doc Start a TCP echo server.
-spec start_link(atom(), integer()) -> ok.
start_link(Name, Port) ->
    gen_tcp_server:start_link(Name, ?MODULE, Port, [{packet, line}]).

%%%-----------------------------------------------------------------------------
%%% gen_tcp_server_handler callbacks
%%%-----------------------------------------------------------------------------

%% @private
handle_accept(_Socket, Name) ->
    {ok, #state{name = Name}}.

%% @private
handle_tcp(_Socket, Data, State) ->
    ebalancer_balancer:receive_data(State#state.name, Data),
    {ok, State}.

%% @private
handle_close(_Socket, _Reason, _State) ->
    ok.
