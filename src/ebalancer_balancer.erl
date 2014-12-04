-module(ebalancer_balancer).
-author("danos").
-author("osense").

-behaviour(gen_server).

%% API
-export([start_link/0, receive_data/2, register_as_worker/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {balanced_nodes = 0,
    flush_timeout = 100,
    buffer_size = 20,
    workers = [],
    next_worker = 0,
    buffer = []}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({global, balancer}, ?MODULE, [], []).

receive_data(From, Data) ->
    gen_server:cast({global, balancer}, {receive_data, From, Data}).

register_as_worker() ->
    gen_server:call({global, balancer}, register_worker).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(register_worker, {Pid, _Tag}, State = #state{workers = Workers}) ->
    {reply, ok, State#state{workers = [Pid | Workers]}}.


handle_cast({receive_data, _From, Data}, State = #state{buffer = List, buffer_size = Limit}) when length(List) >= Limit - 1 ->
    Worker = lists:nth(State#state.next_worker + 1, State#state.workers),
    ebalancer_worker:receive_batch(Worker, [Data | List]),
    NextWorker = ((State#state.next_worker + 1) rem length(State#state.workers)),
    {noreply, State#state{buffer = [], next_worker = NextWorker}};

handle_cast({receive_data, _From, Data}, State = #state{buffer = List}) ->
    {noreply, State#state{buffer = [Data | List]}}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
