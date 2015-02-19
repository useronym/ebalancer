-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, expect/2, collect/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {expected = []}).


%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Tell the collector to expect a snapshot of the system, consisting of 'Count' parts.
expect(Ref, Count) ->
    gen_server:cast(?MODULE, {expect, Ref, Count}).

% Collect an expected part of a snapshot.
collect(Node, Ref, List) ->
    gen_server:cast({?MODULE, Node}, {collect, Ref, List}).


%% -------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({expect, Ref, Count}, State = #state{expected = E}) ->
    {noreply, State#state{expected = [{Ref, Count, []}|E]}};

handle_cast({collect, Ref, List}, State = #state{expected = E}) ->
    case lists:keyfind(Ref, 1, E) of
        {_, Count, Collected} when Count - 1 == 0 ->
            FinalList = lists:flatten([List | Collected]),
            Sorted = lists:sort(fun ({VC1, _, _}, {VC2, _, _}) -> vclock:compare(VC1, VC2) end, FinalList),
            dummy_save(Sorted),
            io:format("got a complete snaphost~n"),
            {noreply, State#state{expected = lists:keydelete(Ref, 1, E)}};
        {_, Count, Collected} ->
            NewE = lists:keyreplace(Ref, 1, E, {Ref, Count - 1, [List | Collected]}),
            {noreply, State#state{expected = NewE}};
        _ ->
            error_logger:error_report(["Collector received unexpected data list"]),
            {noreply, State}
    end.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% -------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------

dummy_save(List) ->
    io:format("~p~n", [List]).
