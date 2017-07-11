%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(flow_falcon).

-export([start/0, stop/0]).

-export([start_link/0]).

-export([flow_acc/2,         %% 次数统计
         flow_val/2,         %% 次数统计
         flow_all/0,         %% 统计流量
         flow_list/0,        %% 所有流量
         flow_list/1,        %% 所有流量
         flow_near/1,        %% 最近流量
         flow_near/0]).      %% 最近流量

-export([falcon/1]).         %% 上传

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(ETS_ACC, '$flow_falcon_acc'). %% 累计 {{type1, type2}, val}
-define(ETS_VAL, '$flow_falcon_val'). %% 当前 {{type1, type2}, val}

-define(TIMEOUT, 1000).

-define(FALCON_CD,     1 * 60). %% 1分钟

-define(MIN_LEN,       5 * 60). %% 5分钟
-define(MIN_NAME_LIST, [one_min_ps, five_min_ps]).
-define(MIN_LEN_LIST,  [60, 5 * 60]).

-define(SECOND, (erlang:system_time(seconds) div 1000)).

-record(state, {start_time = 0, flow_list = [], next_profile = 0, next_falcon = 0}).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

flow_acc(Key, Inc) ->
    catch ets:update_counter(?ETS_ACC, Key, Inc, {Key, 0}).

flow_val(Key, Val) ->
    catch ets:insert(?ETS_VAL, {Key, Val}).

flow_all() ->
    FL = flow_list(),
    [{last_second, proplists:get_value(last_second, FL)},
     {all, proplists:get_value(all, FL)},
     {all, proplists:get_value(all, flow_near())}].

flow_list() ->
    gen_server:call(?MODULE, flow_list).

flow_list(Key) ->
    proplists:get_value(Key, flow_list()).

flow_near() ->
    gen_server:call(?MODULE, flow_near).

flow_near(Key) ->
    proplists:get_value(Key, flow_near()).

%%------------------------------------------------------------------------------
init([]) ->
    ets:new(?ETS_ACC, [named_table, public, {write_concurrency, true}]),
    ets:new(?ETS_VAL, [named_table, public, {write_concurrency, true}]),
    {ok, #state{}, 0}.

handle_call(flow_list, _From, State) ->
    {reply, catch do_flow_list(State), State};
handle_call(flow_near, _From, State) ->
    {reply, catch do_flow_near(State), State};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    erlang:send_after(?TIMEOUT, self(), timeout),
    State1 = do_flow(State),
    State2 = do_falcon(State1),
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
do_flow(State) ->
    Flow = do_get_flow(),
    State#state{flow_list = lists:sublist([Flow | State#state.flow_list], ?MIN_LEN)}.

do_get_flow() -> ets:tab2list(?ETS_VAL) ++ ets:tab2list(?ETS_ACC).

%%------------------------------------------------------------------------------
do_falcon(State) ->
    catch falcon(do_falcon_flow(State)),
    State#state{next_falcon = ?SECOND + ?FALCON_CD}.

do_falcon_flow(State) ->
    Flow = ets:tab2list(?ETS_ACC),
    AddList = do_sub_flow(Flow, do_get_nth_flow(?FALCON_CD + 1, State#state.flow_list)),
    [{val, A, B, C} || {{A, B}, C} <- ets:tab2list(?ETS_VAL)]
    ++ [{acc, A, B, C} || {{A, B}, C} <- ets:tab2list(?ETS_ACC)]
    ++ [{add, A, B, C} || {{A, B}, C} <- AddList].

do_sub_flow(NewList, []) -> NewList;
do_sub_flow(NewList, OldList) ->
    Fun = fun({Key, Count}, Acc) ->
                  case lists:keyfind(Key, 1, OldList) of
                      false -> lists:keystore(Key, 1, Acc, {Key, Count});
                      {_, Count1} -> lists:keystore(Key, 1, Acc, {Key, Count - Count1})
                  end
          end,
    lists:foldl(Fun, [], NewList).

do_get_nth_flow(Nth, List) ->
    case length(List) < Nth of
        false -> lists:nth(Nth, List);
        true -> []
    end.

%%------------------------------------------------------------------------------
do_flow_list(State) ->
    [{last_second, do_get_last(State)}] ++ do_format_flow(do_get_flow()).

do_get_last(State) -> ?SECOND - State#state.start_time.

do_format_flow(Flow) ->
    Fun = fun(F, Acc) ->
                  {Type, Val} = element(1, F),
                  T = erlang:insert_element(1, erlang:delete_element(1, F), Val),
                  case lists:keyfind(Type, 1, Acc) of
                      false -> lists:keystore(Type, 1, Acc, {Type, [T]});
                      {_, List} -> lists:keyreplace(Type, 1, Acc,
                                                    {Type, lists:sort([T | List])})
                  end
          end,
    lists:sort(lists:foldl(Fun, [], Flow)).

do_flow_near(State) ->
    Last = do_get_last(State),
    Flow = do_get_flow(),
    Result = do_list_flow(?MIN_LEN_LIST,
                          State#state.flow_list,
                          Flow,
                          length(State#state.flow_list),
                          do_ps_flow(Flow, Last),
                          []),
    [{last_second, Last},
     list_to_tuple([op] ++ ?MIN_NAME_LIST ++ [acc_min_ps])]
    ++ do_format_flow(Result).

do_list_flow([HLen | T], FlowList, Flow, Len, PsFlow, Acc) ->
    case HLen > Len of
        true ->
            do_list_flow(T, FlowList, Flow, Len, PsFlow, do_append_flow(Acc, PsFlow));
        false ->
            HFlow = do_get_nth_flow(HLen + 1, Acc),
            HPsFlow = do_ps_flow(do_sub_flow(Flow, HFlow), Len),
            do_list_flow(T, FlowList, Flow, Len, PsFlow, do_append_flow(Acc, HPsFlow))
    end;
do_list_flow([], _AccFlow, _Flow, _Len, PsFlow, Acc) ->
    do_append_flow(Acc, PsFlow).

%% per second
do_ps_flow(List, Time) ->
    [{{Type, Val}, Count div Time} || {{Type, Val}, Count} <- List].

%% List:{a,1,2}, New:{a,3} Acc:{a,1,2,3}
do_append_flow([], New) -> New;
do_append_flow(List, []) -> List;
do_append_flow(List, New) ->
    Fun = fun({Key, Count}, Acc) ->
                  E = lists:keyfind(Key, 1, List),
                  [erlang:insert_element(erlang:tuple_size(E) + 1, E, Count) | Acc]
          end,
    lists:foldl(Fun, [], New).

%%------------------------------------------------------------------------------
falcon([]) -> skip;
falcon(List) ->
    case proplists:delete(included_applications, application:get_all_env(flow_falcon)) of
        [] -> skip;
        Props ->
            Post = [{metric, proplists:get_value(metric, Props)},
                    {endpoint, proplists:get_value(endpoint, Props)},
                    {timestamp, ?SECOND},
                    {counterType, 'GAUGE'},
                    {step, ?FALCON_CD}],
            List1 = do_form_post(Post, List, []),
            {ok, ConnPid} = gun:open(proplists:get_value(host, Props), proplists:get_value(port, Props)),
            try
                Ref = gun:post(ConnPid, proplists:get_value(path, Props), [], List1),
                {ok, http} = gun:await_up(ConnPid),
                {response, nofin, 200, _} = gun:await(ConnPid, Ref)
            catch E:R -> {false, {E, R, erlang:get_stacktrace()}}
            after
                gun:close(ConnPid),
                gun:flush(ConnPid)
            end
    end.

do_form_post(Post, [{Key1, Key2, Val} | T], Acc) ->
    Tags = list_to_binary("k1=" ++ atom_to_list(Key1)
                          ++ ",k2=" ++ atom_to_list(Key2)),
    do_form_post(Post, T, [[{value, Val}, {tags, Tags} | Post] | Acc]);
do_form_post(_Time, [], Acc) -> jsx:encode(Acc).

