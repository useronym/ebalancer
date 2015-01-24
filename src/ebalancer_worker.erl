-module(ebalancer_worker).

-behaviour(gen_server).

%% API
-export([start_link/0, receive_batch/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(RECEIVE_TIMEOUT, 2500).
%% How often the worker notifies the network of it's presence
-define(NOTIFY_EVERY, 30000).

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
    timer:send_interval(?NOTIFY_EVERY, notify),
    {ok, #state{}}.

handle_call({receive_batch, Batch}, _From, State) ->
    Processed = dummy_function(Batch),
    ok = gen_server:call({global, ecollector}, {collect, Processed}),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(notify, State) ->
    lists:foreach(fun ebalancer_balancer:self_as_worker/1, [node() | nodes()]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

dummy_function({Id, List}) ->
    random:seed(now()),
    timer:sleep(random:uniform(5000)),
    io:format("Worker finished processing batch No. ~p~n", [Id]),
    {Id, List}.