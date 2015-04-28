-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect_all/0, set_next_node/1, open_socket/0, get_active_nodes/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    active_node = false,
    socket,
    prev_pid,
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
set_next_node(Node) ->
    gen_server:call({?MODULE, Node}, next_node).

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


handle_call(next_node, {PrevPid, _Tag}, State) ->
    {reply, ok, State#state{active_node = true, prev_pid = PrevPid}};

handle_call(is_active_node, _From, State) ->
    {reply, State#state.active_node, State}.


handle_cast(collect, State) when State#state.active_node ->
    VCs = ebalancer_controller:get_all_vclocks(),
    MinVC = hd(lists:sort(fun evc:compare/2, VCs)),

    AllNodes = [node() | nodes()],
    TargetVC = lists:foldl(fun evc:increment/2, MinVC, AllNodes),
    Keys = [rpc:async_call(Node, ebalancer_controller, take_msgs, [TargetVC]) || Node <- AllNodes],
    Replies = [rpc:yield(Key) || Key <- Keys],
    Msgs = lists:append(Replies),

    Ordered = lists:sort(fun ({VC1, _, _}, {VC2, _, _}) -> evc:compare(VC1, VC2) end, Msgs),
    % Assumes Payload ends with \n
    Data = [Payload || {_VC, _From, Payload} <- Ordered],
    ok = gen_tcp:send(State#state.socket, Data),

    NextNode = random(nodes()),
    set_next_node(NextNode),
    MonRef = monitor(process, {?MODULE, NextNode}),

    State#state.prev_pid ! 'COLLECTION_OK',

    {noreply, State#state{active_node = false, next_collector_mon = MonRef}};
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
    set_next_node(NewNextNode),
    MonRef = monitor(process, {?MODULE, NewNextNode}),
    {noreply, State#state{next_collector_mon = MonRef}};

handle_info('COLLECTION_OK', State) ->
    demonitor(State#state.next_collector_mon),
    {noreply, State}.


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
