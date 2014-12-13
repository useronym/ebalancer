-module(ebalancer_worker).

-behaviour(gen_server).

%% API
-export([start_link/0, receive_batch/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(RECEIVE_TIMEOUT, 1000).

-record(state, {}).

%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

receive_batch(Worker, List) ->
    gen_server:call(Worker, {receive_batch, List}, ?RECEIVE_TIMEOUT).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    ebalancer_balancer:register_as_worker(),
    {ok, #state{}}.

handle_call({receive_batch, List}, _From, State) ->
    io:format("worker ~p got ~p items~n", [self(), length(List)]),
    spawn(fun() ->
        lists:map(fun(I) -> erlang:md5(I) end, List),
        io:format("worker finished processing a batch~n")
    end),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.