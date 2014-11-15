-module(tcp_fsm).
-author('saleyn@gmail.com').

-behaviour(gen_fsm).

-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
  handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% FSM States
-export([
  'WAIT_FOR_SOCKET'/2,
  'WAIT_FOR_DATA'/2
]).

-record(state, {
  socket,    % client socket
  addr,       % client address
  module}).

-define(TIMEOUT, 120000).

%%%------------------------------------------------------------------------
%%% API
%%%------------------------------------------------------------------------

%%-------------------------------------------------------------------------
%% @spec (Socket) -> {ok,Pid} | ignore | {error,Error}
%% @doc To be called by the supervisor in order to start the server.
%%      If init/1 fails with Reason, the function returns {error,Reason}.
%%      If init/1 returns {stop,Reason} or ignore, the process is
%%      terminated and the function returns {error,Reason} or ignore,
%%      respectively.
%% @end
%%-------------------------------------------------------------------------
start_link(Mod) ->
  gen_fsm:start_link(?MODULE, [Mod], []).

%%%------------------------------------------------------------------------
%%% Callback functions from gen_server
%%%------------------------------------------------------------------------

%%-------------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, State}          |
%%          {ok, StateName, State, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%% @private
%%-------------------------------------------------------------------------
init([Mod]) ->
  process_flag(trap_exit, true),
  {ok, 'WAIT_FOR_SOCKET', #state{module = Mod}}.

%%-------------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextState}          |
%%          {next_state, NextStateName, NextState, Timeout} |
%%          {stop, Reason, NewState}
%% @private
%%-------------------------------------------------------------------------
'WAIT_FOR_SOCKET'({socket_ready, Socket}, #state{module = Mod} = State) when is_port(Socket) ->
  % Now we own the socket
  inet:setopts(Socket, [{active, once} | Mod:get_sockopts()]),
  {ok, {IP, _Port}} = inet:peername(Socket),
  {next_state, 'WAIT_FOR_DATA', State#state{socket = Socket, addr = IP}, ?TIMEOUT};
'WAIT_FOR_SOCKET'(Other, State) ->
  error_logger:error_msg("State: 'WAIT_FOR_SOCKET'. Unexpected message: ~p\n", [Other]),
  %% Allow to receive async messages
  {next_state, 'WAIT_FOR_SOCKET', State}.

%% Notification event coming from client
'WAIT_FOR_DATA'({data, Data}, #state{socket = S, module = Mod} = State) ->
  ok = Mod:handle_data(Data, S),
  {next_state, 'WAIT_FOR_DATA', State, ?TIMEOUT};

'WAIT_FOR_DATA'(timeout, State) ->
  error_logger:error_msg("~p Client connection timeout - closing.\n", [self()]),
  {stop, normal, State};

%% TODO - How to handle this properly?
'WAIT_FOR_DATA'(Data, State) ->
  error_logger:info_msg("~p Info message received: ~p.\n", [self(), Data]),
  {next_state, 'WAIT_FOR_DATA', State, ?TIMEOUT}.

%%-------------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextState}          |
%%          {next_state, NextStateName, NextState, Timeout} |
%%          {stop, Reason, NewState}
%% @private
%%-------------------------------------------------------------------------
handle_event(Event, StateName, State) ->
  {stop, {StateName, undefined_event, Event}, State}.

%%-------------------------------------------------------------------------
%% Func: handle_sync_event/4
%% Returns: {next_state, NextStateName, NextState}            |
%%          {next_state, NextStateName, NextState, Timeout}   |
%%          {reply, Reply, NextStateName, NextState}          |
%%          {reply, Reply, NextStateName, NextState, Timeout} |
%%          {stop, Reason, NewState}                          |
%%          {stop, Reason, Reply, NewState}
%% @private
%%-------------------------------------------------------------------------
handle_sync_event(Event, _From, StateName, State) ->
  {stop, {StateName, undefined_event, Event}, State}.

%%-------------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextState}          |
%%          {next_state, NextStateName, NextState, Timeout} |
%%          {stop, Reason, NewState}
%% @private
%%-------------------------------------------------------------------------
handle_info({tcp, Socket, Bin}, StateName, #state{socket = Socket} = State) ->
  % Flow control: enable forwarding of next TCP message
  inet:setopts(Socket, [{active, once}]),
  ?MODULE:StateName({data, Bin}, State);

handle_info({tcp_closed, Socket}, _StateName, #state{socket = Socket, addr = Addr} = State) ->
  error_logger:info_msg("~p Client ~p disconnected.\n", [self(), Addr]),
  {stop, normal, State};

handle_info(Info, StateName, State) ->
  ?MODULE:StateName(Info, State).

%%-------------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%% @private
%%-------------------------------------------------------------------------
terminate(_Reason, _StateName, #state{socket = Socket}) ->
  (catch gen_tcp:close(Socket)),
  ok.

%%-------------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewState}
%% @private
%%-------------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.