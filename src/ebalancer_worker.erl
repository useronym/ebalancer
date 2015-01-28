-module(ebalancer_worker).

-behaviour(gen_server).

%% API
-export([start_link/0, receive_batch/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(RECEIVE_TIMEOUT, 1000).
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

handle_call({receive_batch, {Id, Batch}}, _From, State) ->
    spawn(fun() ->
        Processed = lists:map(fun dummy_function/1, Batch),
        gen_server:call({global, ecollector}, {collect, {Id, Processed}}) end),
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

dummy_function(Binary) ->
    re:split(Binary, " ", [{return, binary}]).