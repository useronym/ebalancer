%%%-------------------------------------------------------------------
%%% @author danos
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. VII 2014 19:19
%%%-------------------------------------------------------------------
-module(ebalancer_handler_echo).
-author("danos").

%% API
-export([handle_data/2, get_sockopts/0]).

get_sockopts() ->
  [{packet, 2}, binary, {packet_size, 500}].

handle_data(Data, Socket) when is_port(Socket) ->
  gen_tcp:send(Socket, Data).