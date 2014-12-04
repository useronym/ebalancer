-module(ebalancer_worker).
-author("osense").

-behaviour(gen_server).

%% API
-export([start_link/0, receive_batch/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {}).

%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

receive_batch(Worker, List) ->
    gen_server:call(Worker, {receive_batch, List}).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    ebalancer_balancer:register_as_worker(),
    {ok, #state{}}.

handle_call({receive_batch, List}, _From, State) ->
    io:format("worker ~p got ~p items~n", [self(), length(List)]),
    %% start processing here, but don't block - reply with ok
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.