%%%-------------------------------------------------------------------
%%% @author xtovarn
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. IV 2015 10:41
%%%-------------------------------------------------------------------
-module(rh).
-author("xtovarn").

%% API
-export([rh1/3, test_perf/0, rh2/3]).

rh1(Key, Nodes, _N) ->
	lists:sort(Nodes),
	Fun = fun(Node, {MaxNode, MaxValue}) ->
		Hash = erlang:phash2({Key, Node}),
		case Hash > MaxValue of
			true -> {Node, Hash};
			false -> {MaxNode, MaxValue}
		end
	end,
	{Result, _} = lists:foldl(Fun, {undefined, -1}, Nodes),
	Result.

rh2(Key, Nodes, N) ->
	L = lists:map(fun(Node) -> {erlang:phash2({Key, Node}), Node} end, Nodes),
	lists:sublist(lists:reverse(lists:sort(L)), N).

test_perf() ->
	Nodes = [wpOVXYm7@localhost, bhcEl1lA@localhost, ho867Kea@localhost, uLVveyEU@localhost, hxKaLQJt@localhost, czcygmSN@localhost, nQiDurbv@localhost, wa7HNV78@localhost, zBQV3nK7@localhost, igmpo7HK@localhost, lwSDik8N@localhost, kCUL08B1@localhost, oTUzmJKK@localhost, le2nXy0s@localhost, hUlYX2zH@localhost, iwBcOLou@localhost, wMvkso2i@localhost, gEsDFsrr@localhost, aPsJI4Gx@localhost],
	F =
		fun() ->
			[rh:rh2({balancer1, {1, I}}, Nodes, 1) || I <- lists:seq(1, 150000)]
		end,
	{Time, Value} = timer:tc(F),
	{Time / 1000000, Value}.
