-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {expected = []}).


%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Initiates the collection process.
collect() ->
    gen_server:call(?MODULE, collect).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(collect, _From, State) ->
    Nodes = [node() | nodes()],
    {VCs, []} = rpc:multicall(Nodes, ebalancer_controller, get_vc, []),
    MaxVC = hd(lists:sort(fun vclock:compare/2, VCs)),

    {Replies, []} = rpc:multicall(Nodes, ebalancer_controller, take_msgs, [MaxVC]),
    Data = lists:append(Replies),
    Ordered = lists:sort(fun ({VC1, _}, {VC2, _}) -> vclock:compare(VC1, VC2) end, Data),

    stat:stat(Ordered),
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {reply, ok, State}.


handle_info(_Info, State) ->
    {norepy, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
