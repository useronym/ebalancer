-module(ebalancer_balancer).

-behaviour(gen_server).

%% API
-export([start_link/0, send_tcp/2, send_work/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {vc = evc:new()}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

send_tcp(From, Data) ->
    gen_server:cast(?MODULE, {send_tcp, From, Data}).

send_work(Node, VC, From, Data) ->
    gen_server:cast({?MODULE, Node}, {send_work, VC, {node(), From, Data}}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({send_tcp, From, Data}, State) ->
    NewVC = evc:event(State#state.vc),
    ebalancer_store:store(NewVC, From, Data),
    Nodes = nodes(), % [node() | nodes()],
    TargetNode = lists:nth(random:uniform(length(Nodes)), Nodes),
    ebalancer_balancer:send_work(TargetNode, NewVC, From, Data),
    {noreply, State#state{vc = NewVC}};

handle_cast({send_work, VC, NFD}, State) ->
    NewVC = evc:merge(VC, evc:event(State#state.vc)),
    ebalancer_worker:send_work(VC, NFD),
    {noreply, State#state{vc = NewVC}}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
