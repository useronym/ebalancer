-module(ebalancer_collector).

-behaviour(gen_server).

%% API
-export([start_link/0, collect/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {expected = []}).

%% How often the collection process should be run, in milliseconds.
-define(COLLECT_EVERY, 500).


%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Initiates the collection process.
collect(Node) ->
    gen_server:call({?MODULE, Node}, collect).


%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(collect, _From, State) ->
    Started = now(),
    OtherNodes = nodes(),
    AllNodes = [node() | OtherNodes],

    % First, we need all the vector clocks.
    {VCs, []} = rpc:multicall(AllNodes, ebalancer_controller, get_vc, []),
    MinVC = hd(lists:sort(fun vclock:compare/2, VCs)),

    % Now we make calls to all the nodes and request the messages they have.
    {Replies, []} = rpc:multicall(AllNodes, ebalancer_controller, take_msgs, [MinVC]),

    % At this point, it is safe to tell another node to initiate collection.
    Elapsed = timer:now_diff(now(), Started) div 1000,
    NextNode = random(OtherNodes),
    spawn(fun() ->
                  ToSleep = max(0, ?COLLECT_EVERY - Elapsed),
                  timer:sleep(ToSleep),
                  ebalancer_collector:collect(NextNode)
          end),

    % We order and output the messages.
    Data = lists:append(Replies),
    Ordered = lists:sort(fun ({VC1, _}, {VC2, _}) -> vclock:descends(VC2, VC1) end, Data),
    stat:stat(Ordered),
    garbage_collect(),
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {reply, ok, State}.


handle_info(_Info, State) ->
    {norepy, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-----------------------------------------------------------------------------
%%% internal functions
%%%-----------------------------------------------------------------------------

%% @doc Returns a random element from a list.
random(List) ->
    lists:nth(random:uniform(length(List)), List).