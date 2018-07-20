%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc console flow and to falcon
%%% @end
%%%-------------------------------------------------------------------
-module(flow_util).

-export([format_number/1,
         format_time/1,
         cal_cpu/0,
         to_list/1,
         to_atom/1]).

%%------------------------------------------------------------------------------
format_number(V) ->
    if
        V =< 1000 -> integer_to_list(V);
        V =< 1000 * 1000 -> float_to_list(V / 1000, [{decimals, 3}]) ++ "K";
        V =< 1000 * 1000 * 1000 -> float_to_list(V / 1000 / 1000, [{decimals, 3}]) ++ "M";
        true -> float_to_list(V / 1000 / 1000 / 1000, [{decimals, 3}]) ++ "G"
    end.

format_time(V) ->
    if
        V =< 60 -> integer_to_list(V) ++ "s";
        V =< 60 * 60 -> integer_to_list(V div 60) ++ "m" ++ format_time(V rem 60);
        true -> integer_to_list(V div 3600) ++ "h" ++ format_time(V rem 3600)
    end.

cal_cpu() ->
    erlang:system_flag(scheduler_wall_time, true),
    Ts0 = lists:sort(statistics(scheduler_wall_time)),
    timer:sleep(100),
    Ts1 = lists:sort(statistics(scheduler_wall_time)),
    erlang:system_flag(scheduler_wall_time, false),
    Fun = fun({{I, A0, T0}, {I, A1, T1}}) -> {I, round((A1 - A0) * 100 / (T1 - T0)) / 100} end,
    List = lists:map(Fun, lists:zip(Ts0, Ts1)),
    round(lists:sum([X || {_, X} <- List]) * 100 / length(List)).

to_list(X) when is_atom(X) -> atom_to_list(X);
to_list(X) when is_binary(X) -> binary_to_list(X);
to_list(X) when is_integer(X) -> integer_to_list(X);
to_list(X) when is_list(X) -> X.

to_atom(X) when is_list(X) -> list_to_atom(X);
to_atom(X) when is_binary(X) -> binary_to_atom(X, utf8);
to_atom(X) when is_integer(X) -> to_atom(integer_to_list(X));
to_atom(X) when is_atom(X) -> X.

