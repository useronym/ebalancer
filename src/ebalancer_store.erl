-module(ebalancer_store).

-behaviour(gen_server).

%% API
-export([start_link/0, promise/3, collect/3, mark/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {store = ets:new(table, []),
                saved = []}).

-define(STORE_LIMIT, 10).
-define(CHECK_STORE_EVERY, 1000).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

promise(VC, From, Data) ->
    gen_server:cast(?MODULE, {promise, VC, From, Data}).

collect(Node, VC, Data) ->
    gen_server:cast({?MODULE, Node}, {collect, VC, Data}).

mark(Node, FromNode, Ref) ->
    gen_server:cast({?MODULE, Node}, {mark, FromNode, Ref}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    timer:send_interval(?CHECK_STORE_EVERY, check_store_limit),
    timer:send_interval(?CHECK_STORE_EVERY, check_store_collected),
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({promise, VC, From, Data}, State) ->
                                                % 'false' here means this object still has to be collected
    ets:insert(State#state.store, {VC, From, Data, false}),
    {noreply, State};

handle_cast({collect, VC, Data}, State) ->
    CollectFun = fun(Tab) -> ets:update_element(Tab, VC, [{3, Data}, {4, true}]) end,
    case CollectFun(State#state.store) of
        false ->
            lists:any(fun({Tab, _, _}) -> CollectFun(Tab) end, State#state.saved);
        _ ->
            []
    end,
    {noreply, State};

handle_cast({mark, FromNode, Ref}, State = #state{store = Tab, saved = List}) ->
    {noreply, State#state{store = ets:new(table, []), saved = [{Tab, FromNode, Ref} | List]}}.


handle_info(check_store_limit, State) ->
    case ets:info(State#state.store, size) of
        Size when Size > ?STORE_LIMIT -> %% this is where we create a 'snapshot' of the whole system
            Nodes = [node() | nodes()],
            Ref = make_ref(),
            lists:foreach(fun(Node) -> mark(Node, node(), Ref) end, Nodes),
            ebalancer_collector:expect(Ref, length(Nodes));
        _ ->
            []
    end,
    {noreply, State};

handle_info(check_store_collected, State = #state{saved = Saved}) ->
    fun F([{Tab, From, Ref} | List]) ->
            case table_collected(Tab) of
               true ->
                   List = prepare_table(Tab),
                   ebalancer_collector:collect_list(From, Ref, List),
                   {noreply, State#state{saved = lists:keydelete(Ref, 3, Saved)}};
               _ ->
                   F(List)
           end;
        F([]) -> {noreply, State};
    end,
    F(Saved).


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% -------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------

table_collected(Tab) ->
    ets:match(Tab, {'_', '_', '_', true}) == [].

prepare_table(Tab) ->
    lists:map(fun({VC, From, Data, _}) -> {VC, From, Data} end, ets:tab2list(Tab)).
