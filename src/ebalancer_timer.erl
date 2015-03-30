-module(ebalancer_timer).

-behaviour(gen_server).

%% API
-export([start_link/0, increase/1, reset_all/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    count = 0,
    timer_ref
}).

-define(TIME_LIMIT, 500).
-define(MSG_LIMIT, 5000).

%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Increase the message counter on the local node.
increase(Count) ->
    gen_server:cast(?MODULE, {increase, Count}).

%% Reset timer an all nodes.
reset_all() ->
    gen_server:abcast(?MODULE, reset).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, TRef} = timer:send_after(?TIME_LIMIT, timeout),
    {ok, #state{timer_ref = TRef}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({increase, Inc}, State = #state{count = Counter}) when Counter+Inc > ?MSG_LIMIT ->
    ebalancer_collector:collect_all(),
    ebalancer_timer:reset_all(),
    {noreply, State#state{count = 0}};
handle_cast({increase, Inc}, State = #state{count = Counter}) ->
    {noreply, State#state{count = Counter + Inc}};

handle_cast(reset, State) ->
    timer:cancel(State#state.timer_ref),
    {ok, TRef} = timer:send_after(?TIME_LIMIT, timeout),
    {noreply, State#state{count = 0, timer_ref = TRef}}.


handle_info(timeout, State) ->
    ebalancer_collector:collect_all(),
    ebalancer_timer:reset_all(),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
