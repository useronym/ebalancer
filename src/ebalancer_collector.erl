-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect/0, set_active_node/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    active_node,
    prev_pid,
    next_node_mon
}).


%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Initiates the collection process on some previously selected node.
collect() ->
    gen_server:abcast(?MODULE, collect).

%% Sets the next node that should perform the collection process.
set_active_node(Node) ->
    gen_server:multi_call([node() | nodes()], ?MODULE, {active_node, Node}, 1000).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call({active_node, Node}, {PrevPid, _Tag}, State) ->
    {reply, ok, State#state{active_node = Node, prev_pid = PrevPid}}.


handle_cast(collect, State) when node() == State#state.active_node ->
    % First, we need all the vector clocks.
    VCs = ebalancer_controller:get_all_vclocks(),
    MinVC = hd(lists:sort(fun vclock:compare/2, VCs)),

    % Now we make calls to all the nodes and request the messages they have.
    Msgs = ebalancer_controller:take_all_msgs(MinVC),

    % We order and output the messages.
    Ordered = lists:sort(fun ({VC1, _}, {VC2, _}) -> vclock:compare(VC2, VC1) end, Msgs),
    stat:stat(Ordered),

    % Set the next active node.
    NextNode = random(nodes()),
    set_active_node(NextNode),
    MonRef = monitor(process, {?MODULE, NextNode}),

    % Tell the previous node to stop monitoring us.
    State#state.prev_pid ! 'COLLECTION_OK',

    {noreply, State#state{active_node = undefined, next_node_mon = MonRef}};
handle_cast(collect, State) when node() /= State#state.active_node ->
    {noreply, State}.


handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
    NewNextNode = random(nodes()),
    set_active_node(NewNextNode),
    MonRef = monitor(process, {?MODULE, NewNextNode}),
    {noreply, State#state{next_node_mon = MonRef}};

handle_info('COLLECTION_OK', State) ->
    demonitor(State#state.next_node_mon),
    {noreply, State#state{next_node_mon = undefined}}.


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
