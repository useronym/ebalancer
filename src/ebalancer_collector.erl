-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect_all/0, set_next_node/2, get_active_nodes/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    active_node = false,
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
set_next_node(Node, Vclock) ->
    gen_server:call({?MODULE, Node}, {next_node, Vclock}).

%% Returns a list of nodes that currently think they are collecting.
get_active_nodes() ->
    {Replies, _} = gen_server:multi_call(?MODULE, is_active_node),
    lists:filter(fun({_Node, Active}) -> Active end, Replies).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call({next_node, PrevVclock}, {PrevPid, _Tag}, State) ->
    {reply, ok, State#state{
        active_node = true,
        prev_pid = PrevPid,
        vclock = PrevVclock
    }};

handle_call(is_active_node, _From, State) when State#state.active_node ->
    {reply, true, State};
handle_call(is_active_node, _From, State) when not State#state.active_node ->
    {reply, false, State}.


handle_cast(collect, State) when State#state.active_node ->
    % First, we need the previous collections clock.
    PrevVC = State#state.vclock,
    % And also the current clocks on all nodes.
    VCs = ebalancer_controller:get_all_vclocks(),
    MinVC = hd(lists:sort(fun vclock:compare/2, VCs)),

    % Figure out how many messages we want from each node.
    Counts = [{Node, vclock:get_counter(Node, MinVC) - vclock:get_counter(Node, PrevVC)}
              || Node <- vclock:all_nodes(MinVC)],
    % Make async calls to the nodes.
    Keys = [rpc:async_call(Node, ebalancer_controller, take_msgs, [Count])
            || {Node, Count} <- Counts],
    % Collect the replies.
    Replies = [rpc:yield(Key) || Key <- Keys],
    Msgs = lists:append(Replies),

    % We order and output the messages.
    Ordered = lists:sort(fun ({VC1, _}, {VC2, _}) ->
        vclock:compare(VC1, VC2) end,
        Msgs),
    stat:stat(Ordered),

    % Set the next active node.
    NextNode = random(nodes()),
    set_next_node(NextNode, MinVC),
    MonRef = monitor(process, {?MODULE, NextNode}),

    % Tell the previous node to stop monitoring us.
    State#state.prev_pid ! 'COLLECTION_OK',

    {noreply, State#state{
        active_node = false,
        next_collector_mon = MonRef,
        vclock = MinVC
    }};
handle_cast(collect, State) when not State#state.active_node ->
    {noreply, State}.


handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
    NewNextNode = random(nodes()),
    error_logger:warning_report({"Collecting node crashed, restarting on",
        NewNextNode}),
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
