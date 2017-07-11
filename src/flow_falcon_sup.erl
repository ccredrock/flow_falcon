%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月07日12:11:03
%%%-------------------------------------------------------------------
-module(flow_falcon_sup).

-export([start_link/0]).
-export([init/1]).

%%------------------------------------------------------------------------------
-behaviour(supervisor).

%%------------------------------------------------------------------------------
start_link() ->
    {ok, Sup} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, _} = supervisor:start_child(?MODULE, {flow_falcon,
                                               {flow_falcon, start_link, []},
                                               transient, infinity, worker,
                                               [flow_falcon]}),
    {ok, Sup}.

init([]) ->
    {ok, {{one_for_one, 1, 60}, []}}.

