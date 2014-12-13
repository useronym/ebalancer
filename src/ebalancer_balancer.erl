-module(ebalancer_balancer).

-behaviour(gen_server).

%% API
-export([start_link/0, receive_data/2, register_as_worker/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(PROCNAME, {global, balancer}).

-record(state, {balanced_nodes = 0,
    flush_timeout = 10000,
    buffer_size = 20,
    workers = queue:new(),
    buffer = []}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?PROCNAME, ?MODULE, [], []).

receive_data(From, Data) ->
    gen_server:cast(?PROCNAME, {receive_data, From, Data}).

register_as_worker() ->
    gen_server:call(?PROCNAME, register_worker).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

handle_call(register_worker, {Pid, _Tag}, State = #state{workers = Workers}) ->
    {reply, ok, State#state{workers = queue:in(Pid, Workers)}}.

handle_cast({receive_data, _From, Data}, State = #state{buffer = List, buffer_size = Limit}) when length(List) >= Limit - 1 ->
    {ok, NewState} = dispatch(State#state{buffer = [Data | List]}),
    {noreply, NewState};
handle_cast({receive_data, _From, Data}, State = #state{buffer = List}) ->
    {noreply, State#state{buffer = [Data | List]}, State#state.flush_timeout}.

handle_info(timeout, State) ->
    {ok, NewState} = dispatch(State),
    {noreply, NewState}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% private functions
%%%-----------------------------------------------------------------------------

%% Assumes non-empty worker queue, crashes otherwise.
dispatch(State) ->
    {{value, Worker}, Q} = queue:out(State#state.workers),
    try
        ebalancer_worker:receive_batch(Worker, State#state.buffer),
        NewState = State#state{buffer = [], workers = queue:in(Worker, Q)},
        {ok, NewState}
    catch
        error:E ->
            io:format("dropping worker: failed to contact ~p (~p)~n", [Worker, E]),
            dispatch(State#state{workers = Q})
    end.
