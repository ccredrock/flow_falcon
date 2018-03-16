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

ft()  -> flow_falcon:total().
fl()  -> flow_falcon:flow().
fn()  -> flow_falcon:near().
fl(X) -> flow_falcon:flow(X).
fn(X) -> flow_falcon:near(X).
fp() -> flow_falcon:flow(profile).

-endif.

