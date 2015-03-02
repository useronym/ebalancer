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
    VCs = [ebalancer_controller:get_vc(Node) || Node <- Nodes],
    MaxVC = lists:last(lists:sort(fun vclock:compare/2, VCs)),
    Data = lists:append([ebalancer_controller:get_msgs(Node) || Node <- Nodes]),
    error_logger:info_report({"received", Data}),
    Ordered = lists:sort(fun ({VC1, _, _}, {VC2, _, _}) -> vclock:compare(VC1, VC2) end, Data),
    SplitFun = fun F([{VC, D, N} | Rest], Acc) ->
        case VC == MaxVC of
            false ->
                F(Rest, [{VC, D, N} | Acc]);
            true ->
                [{VC, D, N} | Acc]
        end;
        F([], Acc) ->
            Acc
        end,
    Safe = lists:reverse(SplitFun(Ordered, [])),
    PrettySafe = lists:map(fun({VC, Payload, Node}) -> {Payload, Node, lists:sort(VC)} end, Safe),
    error_logger:info_report({{"cutting off at", MaxVC}, PrettySafe}),
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {reply, ok, State}.


handle_info(_Info, State) ->
    {norepy, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
