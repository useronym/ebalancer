-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {expected = []}).

-define(COLLECT_AFTER, 1000).


%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Initiates the collection process.
%% The collector requests a snapshot of the system to be created,
%% then collects and orders the pieces.
collect() ->
    gen_server:cast(?MODULE, collect).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(collect, State = #state{expected = Expected}) ->
    {Ref, Nodes} = ebalancer_controller:snapshot(),
    timer:send_after(?COLLECT_AFTER, {collect, Ref}),
    {noreply, State#state{expected = [{Ref, Nodes, []} | Expected]}}.


handle_info({collect, Ref}, State = #state{expected = Expected}) ->
    {_, Nodes, PreviouslyCollected} = lists:keyfind(Ref, 1, Expected),
    %% TODO: inefficient, investigate the rpc module
    Replies = [{Node, ebalancer_store:collect(Node, Ref)} || Node <- Nodes],
    {ListOfCollected, LateNodes} = lists:foldl(fun collect_fold_fun/2, {PreviouslyCollected, []}, Replies),
    case LateNodes of
        [] ->
            %error_logger:info_report({"collector", "collected", Ref}),
            Collected = lists:append(ListOfCollected),
            Sorted = lists:sort(fun({VC1, _}, {VC2, _}) -> vclock:compare(VC1, VC2) end, Collected),
            dummy_save(Sorted),
            {noreply, State#state{expected = lists:keydelete(Ref, 1, Expected)}};
        _ ->
            %error_logger:info_report({"collector", "nodes were late", {Ref, length(LateNodes)}}),
            timer:send_after(?COLLECT_AFTER, {collect, Ref}),
            NewExpected = lists:keyreplace(Ref, 1, Expected, {Ref, LateNodes, ListOfCollected}),
            {noreply, State#state{expected = NewExpected}}
    end.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% private functions
%%%-----------------------------------------------------------------------------

collect_fold_fun({_Node, {ok, List}}, {Collected, NotReady}) ->
    {[List | Collected], NotReady};
collect_fold_fun({Node, notready}, {Collected, NotReady}) ->
    {Collected, [Node | NotReady]}.

dummy_save(List) ->
    io:format("got a complete snaphost: ~p~n", [List]).
