-module(ebalancer_balancer).

-behaviour(gen_server).

%% API
-export([start_link/0, receive_tcp/2, receive_work/3, receive_confirm/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {buffer = [],
    vc = evc:new()}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

receive_tcp(From, Data) ->
    gen_server:cast(?MODULE, {receive_tcp, From, Data}).

receive_work(Node, VC, Work) ->
    gen_server:cast({?MODULE, Node}, {receive_work, VC, Work}).

receive_confirm(Node, VC) ->
    gen_server:cast({?MODULE, Node}, {receive_confirm, VC}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({receive_tcp, From, Data}, State) ->
    NewVC = evc:event(State#state.vc),
    Nodes = nodes(), % [node() | nodes()],
    TargetNode = lists:nth(random:uniform(length(Nodes)), Nodes),
    ebalancer_balancer:receive_work(TargetNode, NewVC, {node(), From, Data}),
    {noreply, State#state{buffer = [{NewVC, From, Data} | State#state.buffer], vc = NewVC}};

handle_cast({receive_work, VC, Work}, State) ->
    NewVC = evc:merge(VC, evc:event(State#state.vc)),
    ebalancer_worker:receive_work(VC, Work),
    {noreply, State#state{vc = NewVC}};

handle_cast({receive_confirm, VC}, State) ->
    NewBuffer = lists:keydelete(VC, 1, State#state.buffer),
    {noreply, State#state{buffer = NewBuffer}}.


%% discard messages caused by worker timeouts that were just late with the reply
handle_info({Ref, _}, State) when is_reference(Ref) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
