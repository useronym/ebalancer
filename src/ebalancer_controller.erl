-module(ebalancer_controller).

-behaviour(gen_server).

%% API
-export([start_link/0, send_tcp/2, notify/2, get_all_vclocks/0, take_msgs/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    vc = vclock:fresh(),
    msgs = ets:new(msg_store, [])
}).


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

%% Get the latest VCs on all nodes.
get_all_vclocks() ->
    {Replies, []} = gen_server:multi_call(?MODULE, get_vc),
    [VC || {_Node, VC} <- Replies].

%% Take Count messages from the local node.
take_msgs(MaxVC) ->
    gen_server:call(?MODULE, {take_msgs, MaxVC}).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(get_vc, _From, State) ->
    {reply, State#state.vc, State};

handle_call({take_msgs, MaxVC}, _From, State = #state{msgs = ETS}) ->
    MaxId = vclock:get_counter(node(), MaxVC),
    Taken = ets:select(ETS, [{{'$1','$2','$3', '$4'}, [{'=<', '$1', MaxId}], [{{'$2', '$3', '$4'}}]}]),
    ets:select_delete(ETS, [{{'$1', '_', '_', '_'}, [{'=<', '$1', MaxId}], [true]}]),
    {reply, Taken, State}.


handle_cast({send_tcp, From, Data}, State = #state{msgs = Msgs}) ->
    IncVC = vclock:increment(State#state.vc),
    ets:insert(Msgs, {vclock:get_counter(node(), IncVC), IncVC, From, Data}),
    TargetNode = random(nodes()),
    ebalancer_controller:notify(TargetNode, IncVC),
    {noreply, State#state{vc = IncVC}};

handle_cast({notify, VC}, State) ->
    NewVC = vclock:merge2(VC, State#state.vc),
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