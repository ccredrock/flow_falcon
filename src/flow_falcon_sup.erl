%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <free>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月07日12:11:03
%%%-------------------------------------------------------------------
-module(flow_falcon_sup).

-export([start_link/0, init/1, start_child/1]).

%%------------------------------------------------------------------------------
-behaviour(supervisor).

%%------------------------------------------------------------------------------
start_link() ->
    {ok, Sup} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, _} = supervisor:start_child(?MODULE, {flow_falcon,
                                               {flow_falcon, start_link, []},
                                               transient, infinity, worker,
                                               [flow_falcon]}),
    {ok, _} = supervisor:start_child(?MODULE, {profile,
                                               {flow_falcon2, start_link, [profile]},
                                               transient, infinity, worker,
                                               [flow_falcon]}),
    {ok, Sup}.

start_child(Metric) ->
    supervisor:start_child(?MODULE, {Metric,
                                     {flow_falcon2, start_link, [Metric]},
                                     transient, infinity, worker,
                                     []}).

init([]) ->
    {ok, {{one_for_one, 1, 60}, []}}.

