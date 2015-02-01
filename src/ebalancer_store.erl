-module(ebalancer_store).

-behaviour(gen_server).

%% API
-export([start_link/0, store/3, confirm/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {in = [],
    out = []}).


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
    {noreply, State#state{in = [{VC, From, Data} | State#state.in]}};

handle_cast({confirm, VC}, State = #state{in = In, out = []}) ->
    handle_cast({confirm, VC}, State#state{in = [], out = lists:reverse(In)});
handle_cast({confirm, VC}, State = #state{in = In, out = Out}) ->
    case lists:keytake(VC, 1, Out) of
        false ->
            {noreply, State#state{in = lists:keydelete(VC, 1, In)}};
        {value, _, NewOut} ->
            {noreply, State#state{out = NewOut}}
    end.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
