-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, expect/1, send_list/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {expected = []}).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

expect(Ref) ->
    gen_server:cast(?MODULE, {expect, Ref}).

send_list(Node, Ref, List) ->
    gen_server:cast({?MODULE, Node}, {list, Ref, List}).

%% -------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({expect, Ref}, State = #state{expected = E}) ->
    Count = [node() | nodes()],
    {noreply, State#state{expected = [{Ref, Count, []}|E]}};

handle_cast({list, Ref, List}, State = #state{expected = E}) ->
    case lists:keyfind(Ref, 1, E) of
        {_, Count, L} when Count - 1 == 0 ->
            FinalList = [List|L],
            _Sorted = lists:sort(fun vclock:compare/2, FinalList);
        {_, Count, L} ->
            NewE = lists:keyreplace(Ref, 1, E, {Ref, Count - 1, [List|L]}),
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
