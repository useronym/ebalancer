-module(ebalancer_timer).

-behaviour(gen_server).

%% API
-export([start_link/0, increase/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    count = 0,
    timer_ref
}).

-define(TIME_LIMIT, 100).
-define(MSG_LIMIT, 5000).

%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Increase the message counter on the local node.
increase(Count) ->
    gen_server:cast(?MODULE, {increase, Count}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, TRef} = timer:send_after(?TIME_LIMIT, timeout),
    {ok, #state{timer_ref = TRef}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({increase, Inc}, State = #state{count = Counter}) ->
    NewCount = Counter + Inc,
    if NewCount >= ?MSG_LIMIT ->
        {noreply, trigger(State)};
       NewCount =< ?MSG_LIMIT ->
        {noreply, State#state{count = NewCount}}
    end.


handle_info(timeout, State) ->
    {noreply, trigger(State)}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% internal functions
%%%-----------------------------------------------------------------------------

trigger(State) ->
    ebalancer_collector:collect(),
    timer:cancel(State#state.timer_ref),
    {ok, TRef} = timer:send_after(?TIME_LIMIT, timeout),
    #state{count = 0, timer_ref = TRef}.
