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

% Promise a single piece of data to the store server. Store will keep it inside an ETS table.
promise(VC, From, Data) ->
    gen_server:cast(?MODULE, {promise, VC, From, Data}).

% Collect a previously promised piece of data.
collect(Node, VC, Data) ->
    gen_server:cast({?MODULE, Node}, {collect, VC, Data}).

% Mark - save the current of the store and dispatch it to the 'FromNode' when it's ready.
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
    ets:insert(State#state.store, {VC, From, Data, false}), % 'false' denotes this object is still to be collected
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

handle_cast({mark, FromNode, Ref}, State = #state{store = Tab, saved = Saved}) ->
    {noreply, State#state{store = ets:new(table, []), saved = [{Tab, FromNode, Ref} | Saved]}}.


% Check if the store limit has been reached, if yes, then create a snapshot across the whole system.
handle_info(check_store_limit, State) ->
    case ets:info(State#state.store, size) of
        Size when Size > ?STORE_LIMIT ->
            Nodes = [node() | nodes()],
            Ref = make_ref(),
            lists:foreach(fun(Node) -> mark(Node, node(), Ref) end, Nodes),
            ebalancer_collector:expect(Ref, length(Nodes));
        _ ->
            []
    end,
    {noreply, State};

% Check if any of the saved tables have been completely collected, if yes then dispatch & erase them.
handle_info(check_store_collected, State = #state{saved = Saved}) ->
    F = fun({Tab, From, Ref}, AccIn) ->
            case table_collected(Tab) of
                true ->
                    List = prepare_table(Tab),
                    ebalancer_collector:collect(From, Ref, List),
                    AccIn;
                _ ->
                    [{Tab, From, Ref} | AccIn]
            end
        end,
    NewSaved = lists:foldl(F, [], Saved),
    {noreply, State#state{saved = NewSaved}}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% -------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------

table_collected(Tab) ->
    ets:match(Tab, {'_', '_', '_', false}) == [].

prepare_table(Tab) ->
    lists:map(fun({VC, From, Data, _}) -> {VC, From, Data} end, ets:tab2list(Tab)).
