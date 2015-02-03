-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {next_id = 0,
    pool = []}).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

collect(VC, Node, From, Data) ->
    gen_server:cast(?MODULE, {collect, VC, Node, From, Data}).

%% -------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({collect, VC, Node, _From, _Data}, State) ->
    ebalancer_store:confirm(Node, VC),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

dummy_save_fun(Line) ->
    file:write_file(out, [Line, $\n], [append]).
