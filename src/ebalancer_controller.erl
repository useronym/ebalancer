-module(ebalancer_controller).

-behaviour(gen_server).

%% API
-export([start_link/0, send_tcp/2, notify/2, snapshot/0, mark/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {vc = vclock:fresh(),
    marked = false}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Send raw TCP data into the system.
send_tcp(From, Data) ->
    gen_server:cast(?MODULE, {send_tcp, From, Data}).

% Notify controller on another node, forcing it to update it's vector clock.
notify(Node, VC) ->
    gen_server:cast({?MODULE, Node}, {notify, VC}).

% Initiate a snapshot of the whole system on the local node.
% Returns a reference and list of nodes that have been marked.
snapshot() ->
    gen_server:call(?MODULE, snapshot).

% Mark the given node, the node should save it's data under the given reference.
mark(Node, Ref) ->
    gen_server:cast({?MODULE, Node}, {mark, Ref}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(snapshot, _From, State) ->
    Nodes = [node() | nodes()],
    Ref = make_ref(),
    lists:foreach(fun(Node) -> mark(Node, Ref) end, Nodes),
    {reply, {Ref, Nodes}, State}.


handle_cast({send_tcp, From, Data}, State) ->
    NewVC = vclock:increment(State#state.vc),
    ebalancer_store:store(NewVC, From, Data),
    ebalancer_worker:process(node(), NewVC, Data),
    Balancers = [node() |nodes()],
    TargetNode = lists:nth(random:uniform(length(Balancers)), Balancers),
    ebalancer_controller:notify(TargetNode, NewVC),
    {noreply, State#state{vc = NewVC}};

handle_cast({notify, VC}, State) ->
    NewVC = vclock:merge([VC, vclock:increment(State#state.vc)]),
    {noreply, State#state{vc = NewVC}};

handle_cast({mark, Ref}, State) ->
    ebalancer_store:mark(Ref),
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
