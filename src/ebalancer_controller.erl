-module(ebalancer_controller).

-behaviour(gen_server).

%% API
-export([start_link/0, send_tcp/2, notify/2, get_vc/1, get_msgs/1, test/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {vc = vclock:fresh(),
    msgs = []}).


%%%-----------------------------------------------------------------------------
%%% API functions
%%%-----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Send raw TCP data into the system.
send_tcp(From, Data) ->
    gen_server:cast(?MODULE, {send_tcp, From, Data}).

% Notify controller on another node, forcing it to update it's vector clock.
notify(Node, VC) ->
    gen_server:cast({?MODULE, Node}, {notify, VC}).

get_vc(Node) ->
    gen_server:call({?MODULE, Node}, get_vc).

get_msgs(Node) ->
    gen_server:call({?MODULE, Node}, get_msgs).

test(Length) ->
    test(1, Length + 1).
test(N, N) ->
    ok;
test(N, Length) ->
    Targets = [node() | nodes()],
    gen_server:cast({?MODULE, lists:nth(random:uniform(length(Targets)), Targets)}, {send_tcp, test, N}),
    timer:sleep(1),
    test(N + 1, Length).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call(get_vc, _From, State) ->
    {reply, State#state.vc, State};

handle_call(get_msgs, _From, State) ->
    {reply, State#state.msgs, State}.


handle_cast({send_tcp, _From, Data}, State) ->
    NewVC = vclock:increment(State#state.vc),
    Others = nodes(),
    TargetNode = lists:nth(random:uniform(length(Others)), Others),
    ebalancer_controller:notify(TargetNode, NewVC),
    {noreply, State#state{vc = NewVC, msgs = [{NewVC, Data, node()} | State#state.msgs]}};

handle_cast({notify, VC}, State) ->
    NewVC = vclock:merge([VC, State#state.vc]),
    {noreply, State#state{vc = NewVC}}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
