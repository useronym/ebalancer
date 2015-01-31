-module(ebalancer_store).

-behaviour(gen_server).

%% API
-export([start_link/0, store/3, confirm/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {store = squeue:new()}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

store(VC, From, Data) ->
    gen_server:cast(?MODULE, {store, VC, From, Data}).

confirm(Node, VC) ->
    gen_server:cast({?MODULE, Node}, {confirm, VC}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({store, VC, From, Data}, State) ->
    {noreply, State#state{store = squeue:in({VC, From, Data}, State#state.store)}};

handle_cast({confirm, VC}, State) ->
    NewBuffer = squeue:keydelete(VC, 1, State#state.store),
    {noreply, State#state{store = NewBuffer}}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
