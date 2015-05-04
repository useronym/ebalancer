-module(ebalancer_controller).

-behaviour(gen_server).

%% API
-export([start_link/0, send_tcp/2, notify/2, get_vclock/1, get_master_node/0, get_all_vclocks/0, take_msgs/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    vc = undefined,
    msgs = [],
    buffer = []
}).

%% When this limit is reached, the controller will request a collection process.
-define(MSG_LIMIT, 5000).

%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Send raw TCP data into the system.
send_tcp(From, Data) ->
    gen_server:cast(?MODULE, {send_tcp, From, Data}).

%% Notify controller on another node, forcing it to update it's vector clock.
notify(Node, VC) ->
    gen_server:cast({?MODULE, Node}, {notify, VC}).

%% Get vclock from the given node
get_vclock(Node) ->
    gen_server:call({?MODULE, Node}, get_vc).

%% Find the node whose node_index is 1.
get_master_node() ->
    {Replies, _} = gen_server:multi_call(nodes(), ?MODULE, get_index, 5000),
    {Node, 1} = lists:keyfind(1, 2, Replies),
    Node.

%% Get the latest VCs on all nodes.
get_all_vclocks() ->
    {Replies, []} = gen_server:multi_call([node() | nodes()], ?MODULE, get_vc, 5000),
    [VC || {_Node, VC} <- Replies].

%% Take messages from the local node until a given point.
take_msgs(MaxVC) ->
    gen_server:call(?MODULE, {take_msgs, MaxVC}).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    Vclock = case application:get_env(node_index) of
        {ok, 1} -> evc:new(node1);
        {ok, N} ->
            timer:sleep(1000), % give the starting master node some time
            MasterVC = {_, Delta, _} = get_vclock(get_master_node()),
            evc:merge(evc:new(list_to_atom("node" ++ integer_to_list(N))), MasterVC, Delta)
    end,
    {ok, #state{vc = Vclock}}.


handle_call(get_vc, _From, State) ->
    {reply, State#state.vc, State};

handle_call(get_index, _From, State) ->
    {ok, Index} = application:get_env(node_index),
    {reply, Index, State};

handle_call({take_msgs, MaxVC}, _From, State) ->
    NewMsgs = lists:append(State#state.msgs, lists:reverse(State#state.buffer)),
    case NewMsgs of
        [] ->
            {reply, [], State};
        _ ->
            {MinVC, _, _} = hd(NewMsgs),
            Count = evc:counter(evc:node_id(MinVC), MaxVC) - evc:counter(MinVC),
            {Taken, Left} = lists:split(max(0, Count), NewMsgs),
            {reply, Taken, State#state{msgs = Left, buffer = []}}
    end.


handle_cast({send_tcp, From, Data}, State = #state{buffer = Buffer}) ->
    IncVC = evc:increment(State#state.vc),
    ebalancer_controller:notify(random(nodes()), IncVC),
    {noreply, State#state{vc = IncVC, buffer = [{IncVC, From, Data} | Buffer]}};

handle_cast({notify, VC}, State) ->
    NewVC = evc:merge(State#state.vc, VC, 0),
    {noreply, State#state{vc = NewVC}}.


handle_info(_Info, State) ->
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