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

receive_batch(Worker, Batch) ->
    gen_server:call(Worker, {receive_batch, Batch}, ?RECEIVE_TIMEOUT).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    ok = ebalancer_balancer:register_as_worker(),
    {ok, #state{}}.

handle_call({receive_batch, Batch}, _From, State) ->
    spawn(fun() ->
        Processed = dummy_function(Batch),
        gen_server:call({global, ecollector}, {collect, Processed}) end),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

dummy_function({Id, List}) ->
    Processed = lists:map(fun erlang:md5/1, List),
    random:seed(now()),
    timer:sleep(random:uniform(5000)),
    io:format("Worker finished processing batch No. ~p~n", [Id]),
    {Id, Processed}.