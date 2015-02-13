-module(ebalancer_balancer).

-behaviour(gen_server).

%% API
-export([start_link/0, send_tcp/2, notify/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {vc = vclock:fresh(),
    marked = false}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

send_tcp(From, Data) ->
    gen_server:cast(?MODULE, {send_tcp, From, Data}).

notify(Node, VC) ->
    gen_server:cast({?MODULE, Node}, {notify, VC}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({send_tcp, From, Data}, State) ->
    NewVC = vclock:increment(State#state.vc),
    ebalancer_store:promise(NewVC, From, Data),
    Balancers = nodes(),
    TargetNode = lists:nth(random:uniform(length(Balancers)), Balancers),
    ebalancer_balancer:notify(TargetNode, NewVC),
    ebalancer_worker:process(node(), NewVC, Data),
    {noreply, State#state{vc = NewVC}};

handle_cast({notify, VC}, State) ->
    NewVC = vclock:merge([VC, vclock:increment(State#state.vc)]),
    {noreply, State#state{vc = NewVC}}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
