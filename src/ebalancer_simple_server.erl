%%%-------------------------------------------------------------------
%%% @author xtovarn
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. V 2014 10:16
%%%-------------------------------------------------------------------
-module(ebalancer_simple_server).
-author("xtovarn").

%% API
-export([start/2, server/1]).

start(Num, LPort) ->
	case gen_tcp:listen(LPort, [binary, {active, false}, {packet, line}]) of
		{ok, ListenSock} ->
			start_servers(Num, ListenSock),
			{ok, Port} = inet:port(ListenSock),
			Port;
		{error, Reason} ->
			{error, Reason}
	end.

start_servers(0, _) ->
	ok;
start_servers(Num, LS) ->
	spawn(?MODULE, server, [LS]),
	start_servers(Num - 1, LS).

server(LS) ->
	case gen_tcp:accept(LS) of
		{ok, S} ->
			loop(S),
			server(LS);
		Other ->
			io:format("accept returned ~w - goodbye!~n", [Other]),
			ok
	end.

loop(S) ->
	inet:setopts(S, [{active, once}]),
	receive
		{tcp, S, Data} ->
%% 			Answer = process(Data), % Not implemented in this example
%% 			gen_tcp:send(S, Answer),
%% 			<<60,_:3/binary,62,_/binary>> = <<"<150>Chaclos net fut marches papiers. \r\n">>,

			io:format("Oh my tcp: ~p~n", [Data]),
			loop(S);
		{tcp_closed, S} ->
			io:format("Socket ~w closed [~w]~n", [S, self()]),
			ok
	end.