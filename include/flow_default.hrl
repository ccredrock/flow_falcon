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

-export([ft/0, fl/0, fn/0, fl/1, fn/1, fp/0]).

ft()  -> flow_falcon:list_total().
fl()  -> flow_falcon:list_flow().
fn()  -> flow_falcon:list_near().
fl(X) -> flow_falcon:list_flow(X).
fn(X) -> flow_falcon:list_near(X).
fp() -> flow_falcon:list_flow(profile).

-endif.

