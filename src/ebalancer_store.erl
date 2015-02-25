-module(ebalancer_store).

-behaviour(gen_server).

%% API
-export([start_link/0, collect/2, store/3, update/3, mark/1]).

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

% Attempt to collect state stored under the given reference.
% Only succesfull when the state has been completely processed.
collect(Node, Ref) ->
    gen_server:call({?MODULE, Node}, {collect, Ref}).

% Promise a single piece of data to the store server.
% Store keeps the raw data, in case the workers lose it.
store(VC, From, Data) ->
    gen_server:cast(?MODULE, {store, VC, From, Data}).

% Collect a previously promised piece of data.
update(Node, VC, Data) ->
    gen_server:cast({?MODULE, Node}, {update, VC, Data}).

% Mark - save the the store under the given reference on the local node.
mark(Ref) ->
    gen_server:cast(?MODULE, {mark, Ref}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call({collect, Ref}, _From, State = #state{saved = Saved}) ->
    case lists:keyfind(Ref, 1, Saved) of
        {Tab, _} ->
            case table_collected(Tab) of
                true ->
                    List = prepare_table(Tab),
                    ets:delete(Tab),
                    {reply, {ok, List}, State#state{saved = lists:keydelete(Ref, 1, Saved)}};
                false ->
                    {reply, notready, State}
            end;
        false ->
            {reply, enoref, State}
    end.


handle_cast({store, VC, From, Data}, State) ->
    ets:insert(State#state.store, {VC, false, From, Data}), % 'false' denotes this object is still to be collected
    {noreply, State};

handle_cast({update, VC, Data}, State) ->
    UpdateFun = fun(Tab) -> ets:update_element(Tab, VC, [{2, true}, {3, Data}]) end,
    case UpdateFun(State#state.store) of
        false ->
            lists:any(fun({_, Tab}) -> UpdateFun(Tab) end, State#state.saved);
        _ ->
            []
    end,
    {noreply, State};

handle_cast({mark, Ref}, State = #state{store = Tab, saved = Saved}) ->
    {noreply, State#state{store = ets:new(table, []), saved = [{Ref, Tab} | Saved]}}.


handle_info(_Info, State) ->
    {noreply, State}.


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
    lists:map(fun({VC, _, From, Data}) -> {VC, {From, Data}} end, ets:tab2list(Tab)).
