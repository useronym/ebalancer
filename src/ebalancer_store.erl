-module(ebalancer_store).

-behaviour(gen_server).

%% API
-export([start_link/0, promise/3, collect/3, mark/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {store = ets:new(table, []),
    saved = []}).

-define(STORE_LIMIT, 10).
-define(CHECK_LIMIT_EVERY, 1000).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

promise(VC, From, Data) ->
    gen_server:cast(?MODULE, {promise, VC, From, Data}).

collect(Node, VC, Data) ->
    gen_server:cast({?MODULE, Node}, {collect, VC, Data}).

mark(Node, Ref) ->
    gen_server:cast({?MODULE, Node}, {mark, Ref}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    timer:send_interval(?CHECK_LIMIT_EVERY, check_store),
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({promise, VC, From, Data}, State) ->
    % 'false' here means this object still has to be collected
    ets:insert(State#state.store, {VC, From, Data, false}),
    {noreply, State};

handle_cast({collect, VC, Data}, State) ->
    CollectFun = fun(T) -> ets:update_element(T, VC, [{3, Data}, {4, true}]) end,
    case CollectFun(State#state.store) of
        false ->
            lists:any(CollectFun, State#state.saved);
        _ ->
            []
    end,
    {noreply, State};

handle_cast({mark, Ref}, State = #state{store = S, saved = List}) ->
    {noreply, State#state{store = ets:new(table, []), saved = [{S, Ref} | List]}}.


handle_info(check_store, State) ->
    case ets:info(State#state.store, size) of
        Size when Size >= ?STORE_LIMIT ->
            Ref = make_ref(),
            lists:foreach(fun(N) -> mark(N, Ref) end, [node() | nodes()]),
            ebalancer_collector:expect(Ref);
        _ ->
            []
    end,
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
