%%%-------------------------------------------------------------------
%%% @author wanghongyan05
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月17日10:24:59
%%%-------------------------------------------------------------------
-ifndef(FLOW_DEFAULT_HRL).
-define(FLOW_DEFAULT_HRL, true).

-export([ft/0, fl/0, fn/0, fl/1, fn/1, fp/0,
         fc/0, fc/1, fv/0, fv/1, fm/1, fm/2]).

ft()  -> flow_falcon:list_total().
fl()  -> flow_falcon:list_flow().
fn()  -> flow_falcon:list_near().
fl(X) -> flow_falcon:list_flow(X).
fn(X) -> flow_falcon:list_near(X).
fp()  -> flow_falcon:list_flow(profile).

fc()  -> flow_falcon2:list_count().
fc(V) -> flow_falcon2:list_count(V).
fv()  -> flow_falcon2:list_value().
fv(V) -> flow_falcon2:list_value(V).
fm(M) -> flow_falcon2:list_metric(M).
fm(M, V) -> flow_falcon2:list_metric(M, V).

-endif.

