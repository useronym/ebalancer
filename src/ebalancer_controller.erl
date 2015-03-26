-module(ebalancer_controller).

-behaviour(gen_server).

%% API
-export([start_link/0, send_tcp/2, notify/2, get_all_vclocks/0, take_all_msgs/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    vc = vclock:fresh(),
    msgs = [],
    buffer = []
}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Send raw TCP data into the system.
send_tcp(From, Data) ->
    gen_server:cast(?MODULE, {send_tcp, From, Data}).

%% Notify controller on another node, forcing it to update it's vector clock.
notify(Node, VC) ->
    gen_server:cast({?MODULE, Node}, {notify, VC}).

%% Get the latest VCs on all nodes.
get_all_vclocks() ->
    {Replies, []} = gen_server:multi_call(?MODULE, get_vc),
    [VC || {_Node, VC} <- Replies].

%% Take messages until a given VC on all nodes.
take_all_msgs(Until) ->
    {Replies, []} = gen_server:multi_call(?MODULE, {take_msgs, Until}),
    {_, ListsOfMsgs} = lists:unzip(Replies),
    lists:append(ListsOfMsgs).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(get_vc, _From, State) ->
    {reply, State#state.vc, State};

handle_call({take_msgs, Until}, _From, State = #state{msgs = Msgs, buffer = Buffer}) ->
    NewMsgs = lists:append(Msgs, lists:reverse(Buffer)),
    {Taken, Left} = lists:splitwith(fun({VC, _}) -> vclock:compare(VC, Until) end, NewMsgs),
    {reply, Taken, State#state{msgs = Left, buffer = []}}.


handle_cast({send_tcp, _From, Data}, State = #state{buffer = Buffer}) ->
    IncVC = vclock:increment(State#state.vc),
    Others = nodes(),
    TargetNode = random(Others),
    ebalancer_controller:notify(TargetNode, IncVC),
    {noreply, State#state{vc = IncVC, buffer = [{IncVC, Data} | Buffer]}};


handle_cast({notify, VC}, State) ->
    NewVC = vclock:merge2(VC, State#state.vc),
    {noreply, State#state{vc = NewVC}}.


handle_info(_Info, State) ->
    {noreply, State}.


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