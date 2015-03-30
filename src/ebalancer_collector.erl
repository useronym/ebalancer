-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect_all/0, set_next_node/2, open_socket/0, get_active_nodes/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    active_node = false,
    socket,
    prev_pid,
    vclock,
    next_collector_mon
}).


%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Initiates the collection process on some previously selected node.
collect_all() ->
    gen_server:abcast(?MODULE, collect).

%% Sets the next node that should perform the collection process.
%% The node also needs to know the last collection's vclock.
set_next_node(Node, PrevVclock) ->
    gen_server:call({?MODULE, Node}, {next_node, PrevVclock}).

%% Tell the collector on local node to open up it's output socket.
open_socket() ->
    gen_server:cast(?MODULE, open_socket).

%% Returns a list of nodes that currently think they are collecting.
get_active_nodes() ->
    {Replies, _} = gen_server:multi_call(?MODULE, is_active_node),
    lists:filter(fun({_Node, Active}) -> Active end, Replies).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    open_socket(),
    {ok, #state{}}.


handle_call({next_node, PrevVclock}, {PrevPid, _Tag}, State) ->
    {reply, ok, State#state{active_node = true, prev_pid = PrevPid, vclock = PrevVclock}};

handle_call(is_active_node, _From, State) when State#state.active_node ->
    {reply, true, State};
handle_call(is_active_node, _From, State) when not State#state.active_node ->
    {reply, false, State}.


handle_cast(collect, State) when State#state.active_node ->
    % First, we need the previous collection's clock.
    PrevVC = State#state.vclock,
    % And also the current clocks on all nodes.
    VCs = ebalancer_controller:get_all_vclocks(),
    MinVC = hd(lists:sort(fun vclock:compare/2, VCs)),

    % Figure out how many messages we want from each node.
    Counts = [{Node, vclock:get_counter(Node, MinVC) - vclock:get_counter(Node, PrevVC)}
        || Node <- vclock:all_nodes(MinVC)],
    % Make async calls to the nodes.
    Keys = [rpc:async_call(Node, ebalancer_controller, take_msgs, [Count]) || {Node, Count} <- Counts],
    % Collect the replies.
    Replies = [rpc:yield(Key) || Key <- Keys],
    Msgs = lists:append(Replies),

    % We order and output the messages.
    Ordered = lists:sort(fun ({VC1, _}, {VC2, _}) -> vclock:compare(VC1, VC2) end, Msgs),
    Data = [Payload || {_VC, Payload} <- Ordered],
    ok = gen_tcp:send(State#state.socket, Data),

    % Set the next active node.
    NextNode = random(nodes()),
    set_next_node(NextNode, MinVC),
    MonRef = monitor(process, {?MODULE, NextNode}),

    % Tell the previous node to stop monitoring us.
    State#state.prev_pid ! 'COLLECTION_OK',

    {noreply, State#state{active_node = false, next_collector_mon = MonRef, vclock = MinVC}};
handle_cast(collect, State) when not State#state.active_node ->
    {noreply, State};

handle_cast(open_socket, State) ->
    {ok, Host} = application:get_env(collector_out_host),
    {ok, Port} = application:get_env(collector_out_port),
    case gen_tcp:connect(Host, Port, [{mode, binary}, {send_timeout, 1000}], 1000) of
        {ok, Socket} ->
            {noreply, State#state{socket = Socket}};
        Error ->
            timer:sleep(1000),
            {stop, Error, State}
    end.


handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
    NewNextNode = random(nodes()),
    error_logger:warning_report({"Collecting node crashed, restarting on", NewNextNode}),
    set_next_node(NewNextNode, State#state.vclock),
    MonRef = monitor(process, {?MODULE, NewNextNode}),
    {noreply, State#state{next_collector_mon = MonRef}};

handle_info('COLLECTION_OK', State) ->
    demonitor(State#state.next_collector_mon),
    {noreply, State#state{next_collector_mon = undefined}}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% internal functions
%%%-----------------------------------------------------------------------------

%% @doc Returns a random element from a list.
random(List) ->
    lists:nth(random:uniform(length(List)), List).
