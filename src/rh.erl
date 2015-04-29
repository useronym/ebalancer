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
-export([test_perf/0, rhash/3]).

%% Key to be hashed, Nodes list, Number of nodes to be returned
rhash(_Key, Nodes, N) when N > length(Nodes) ->
	error(not_implemented);
rhash(Key, Nodes, N) ->
	L = lists:map(fun(Node) -> {erlang:phash2({Key, Node}), Node} end, Nodes),
	lists:sublist(lists:reverse(lists:sort(L)), N).

test_perf() ->
	Nodes = [wpOVXYm7@localhost, bhcEl1lA@localhost, ho867Kea@localhost, uLVveyEU@localhost, hxKaLQJt@localhost, czcygmSN@localhost, nQiDurbv@localhost, wa7HNV78@localhost, zBQV3nK7@localhost, igmpo7HK@localhost, lwSDik8N@localhost, kCUL08B1@localhost, oTUzmJKK@localhost, le2nXy0s@localhost, hUlYX2zH@localhost, iwBcOLou@localhost, wMvkso2i@localhost, gEsDFsrr@localhost, aPsJI4Gx@localhost],
	F =
		fun() ->
			[rh:rhash({balancer1, {1, I}}, Nodes, 1) || I <- lists:seq(1, 150000)]
		end,
	{Time, _} = timer:tc(F),
	io:format("~p seconds~n", [Time / 1000000]).
