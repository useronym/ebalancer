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
    Ordered = lists:sort(fun ({VC1, _}, {VC2, _}) -> vclock:compare(VC1, VC2, fun vclock:get_oldest_timestamp/1) end, Data),
    SplitFun = fun F([{VC, Msg} | Rest], Acc) ->
        case VC == MaxVC of
            false ->
                F(Rest, [{VC, Msg} | Acc]);
            true ->
                [{VC, Msg} | Acc]
        end;
        F([], Acc) ->
            Acc
        end,
    Safe = lists:reverse(SplitFun(Ordered, [])),
    error_logger:info_report({stat:all(Safe)}),
    lists:foreach(fun(Node) -> ebalancer_controller:erase_until(Node, MaxVC) end, Nodes),
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {reply, ok, State}.


handle_info(_Info, State) ->
    {norepy, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
