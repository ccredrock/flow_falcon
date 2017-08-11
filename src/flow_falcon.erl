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

-export([add_acc/3,     %% 次数累计
         set_acc/3,     %% 次数累计
         set_val/3,     %% 次数设置
         inc_total/1,   %% 次数累计
         inc_total/2,   %% 次数累计
         add_total/2,   %% 次数累计
         add_total/3,   %% 次数累计
         set_total/2,   %% 次数设置
         set_total/3,   %% 次数累计
         list_total/0,  %% 统计流量
         list_flow/0,   %% 所有流量
         list_flow/1,   %% 所有流量
         list_near/1,   %% 最近流量
         list_near/0]). %% 最近流量

-export([falcon/1,           %% 上传列表
         falcon_cnt/0]).     %% 上传次数

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(ETS_ACC,     '$flow_falcon_acc'). %% 累计 {{type1, type2}, val}
-define(ETS_VAL,     '$flow_falcon_val'). %% 当前 {{type1, type2}, val}

-define(TIMEOUT, 1000).

-define(FALCON_CD,     1 * 60). %% 1分钟

-define(MIN_LEN,       5 * 60 + 1). %% 5分钟
-define(MIN_NAME_LIST, [one_ps, five_ps]).
-define(MIN_LEN_LIST,  [60, 5 * 60]).

-define(SECOND(), erlang:system_time(seconds)).

-record(state, {start_time = 0,
                acc_list = [],
                val_map = #{},
                next_profile = 0, next_falcon = 0}).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_acc(OP, Type, Inc) ->
    catch ets:update_counter(?ETS_ACC, {OP, Type}, Inc, {{OP, Type}, 0}).

set_acc(OP, Type, Val) ->
    catch ets:insert(?ETS_ACC, {{OP, Type}, Val}).

set_val(OP, Type, Val) ->
    catch ets:update_counter(?ETS_VAL, {OP, Type}, [{1, 1}, {2, Val}], {{OP, Type}, 0, 0}).

inc_total(OP) ->
    add_total(OP, 1).

inc_total(OP, Type) ->
    add_total(OP, Type, 1).

add_total(OP, Inc) ->
    add_acc(total, OP, Inc).

add_total(OP, Type, Inc) ->
    add_acc(total, OP, Inc),
    add_acc(OP, Type, Inc).

set_total(OP, Val) ->
    set_val(total, OP, Val).

set_total(OP, Type, Val) ->
    set_val(total, OP, Val),
    set_val(OP, Type, Val).

list_total() ->
    FL = list_flow(),
    [{last_second, proplists:get_value(last_second, FL, 0)},
     {total, proplists:get_value(total, FL, [])},
     {total, proplists:get_value(total, list_near(), [])}].

list_flow() ->
    gen_server:call(?MODULE, flow_list, ?TIMEOUT).

list_flow(OP) ->
    proplists:get_value(OP, list_flow()).

list_near() ->
    gen_server:call(?MODULE, flow_near, ?TIMEOUT).

list_near(OP) ->
    proplists:get_value(OP, list_near()).

falcon_cnt() ->
    proplists:get_value(falcon_cnt, proplists:get_value(?MODULE, list_flow())).

%%------------------------------------------------------------------------------
init([]) ->
    ets:new(?ETS_ACC, [named_table, public, {write_concurrency, true}]),
    ets:new(?ETS_VAL, [named_table, public, {write_concurrency, true}]),
    {ok, #state{start_time = ?SECOND()}, 0}.

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
    State1 = do_flow(State),
    State2 = do_falcon(State1),
    erlang:send_after(?TIMEOUT, self(), timeout),
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
do_flow(State) ->
    List = [ets:take(?ETS_VAL, K) || {K, _V} <- ets:tab2list(?ETS_VAL)],
    Flow = ets:tab2list(?ETS_ACC),
    State#state{val_map = maps:from_list([{K, Acc div Cnt} || {K, {Cnt, Acc}} <- List]),
                acc_list = lists:sublist([Flow | State#state.acc_list], ?MIN_LEN)}.

do_flow_sys() ->
    List = do_get_cpu(),
    Cpu = round(lists:sum([X || {_, X} <- List]) * 100 / length(List)),
    set_val(profile, cpu, Cpu),
    set_val(profile, total_memory, erlang:memory(total)),
    set_val(profile, process_memory, erlang:memory(processes)),
    set_val(profile, binary_memory, erlang:memory(binary)),
    {{_, Input}, {_, Output}} = erlang:statistics(io),
    set_acc(profile, io_input, Input),
    set_acc(profile, io_output, Output),
    set_val(profile, process_count, erlang:system_info(process_count)),
    MsgQ = lists:sum([Y || {message_queue_len, Y} <- [process_info(X, message_queue_len) || X <- processes()]]),
    set_val(profile, msg_queue, MsgQ).

do_get_cpu() ->
   erlang:system_flag(scheduler_wall_time, true),
   Ts0 = lists:sort(statistics(scheduler_wall_time)),
   timer:sleep(100),
   Ts1 = lists:sort(statistics(scheduler_wall_time)),
   erlang:system_flag(scheduler_wall_time, false),
   Fun = fun({{I, A0, T0}, {I, A1, T1}}) ->
                 {I, round((A1 - A0) * 100 / (T1 - T0)) / 100}
         end,
   lists:map(Fun, lists:zip(Ts0, Ts1)).

%%------------------------------------------------------------------------------
do_falcon(State) ->
    Now = ?SECOND(),
    case Now >= State#state.next_falcon of
        false -> State;
        true ->
            catch do_flow_sys(),
            case falcon(do_falcon_flow(State)) of
                ok ->
                    add_acc(profile, falcon_cnt, 1),
                    State#state{next_falcon = Now + ?FALCON_CD};
                {error, Reason} ->
                    error_logger:error_msg("flow_falcon error ~p~n", [{Reason}]),
                    State#state{next_falcon = Now + ?FALCON_CD}
            end
    end.

do_falcon_flow(State) ->
    Flow = ets:tab2list(?ETS_ACC),
    AddList = do_sub_flow(Flow, do_get_nth_flow(?FALCON_CD + 1, State#state.acc_list)),
    [{val, A, B, C} || {{A, B}, C} <- maps:to_list(State#state.val_map)]
    ++ [{acc, A, B, C} || {{A, B}, C} <- Flow]
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
    List = maps:to_list(State#state.val_map) ++ ets:tab2list(?ETS_ACC),
    [{last_second, do_get_last(State)}] ++ do_format_flow(List).

do_get_last(State) -> ?SECOND() - State#state.start_time.

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
    Flow = ets:tab2list(?ETS_ACC),
    Result = do_list_flow(?MIN_LEN_LIST,
                          State#state.acc_list,
                          Flow,
                          length(State#state.acc_list),
                          do_ps_flow(Flow, Last),
                          []),
    [{last_second, Last}, list_to_tuple([op] ++ ?MIN_NAME_LIST ++ [acc_ps])] ++ do_format_flow(Result).

do_list_flow([HLen | T], FlowList, Flow, Len, PsFlow, Acc) ->
    case HLen > Len of
        true ->
            do_list_flow(T, FlowList, Flow, Len, PsFlow, do_append_flow(Acc, PsFlow));
        false ->
            HFlow = do_get_nth_flow(HLen + 1, FlowList),
            HPsFlow = do_ps_flow(do_sub_flow(Flow, HFlow), HLen),
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
falcon([]) -> ok;
falcon(List) ->
    case application:get_env(flow_falcon, falcon) of
        undefine -> skip;
        {ok, Props} ->
            {ok, HostName} = inet:gethostname(),
            Post = [{metric, list_to_binary(proplists:get_value(metric, Props))},
                    {endpoint, list_to_binary(proplists:get_value(endpoint, Props, HostName))},
                    {timestamp, ?SECOND()},
                    {counterType, 'GAUGE'},
                    {step, ?FALCON_CD}],
            List1 = do_form_post(Post, List, []),
            {ok, ConnPid} = gun:open(proplists:get_value(host, Props),
                                     proplists:get_value(port, Props)),
            try
                {ok, http} = gun:await_up(ConnPid),
                Ref = gun:post(ConnPid, proplists:get_value(path, Props), [], List1),
                {response, nofin, 200, _} = gun:await(ConnPid, Ref), ok
            catch E:R -> {error, {E, R, erlang:get_stacktrace()}}
            after
                gun:close(ConnPid),
                gun:flush(ConnPid)
            end
    end.

do_form_post(Post, [{Way, OP, Type, Val} | T], Acc) ->
    Tags = iolist_to_binary(["way=", atom_to_list(Way),
                             ",op=", atom_to_list(OP),
                             ",type=", atom_to_list(Type)]),
    do_form_post(Post, T, [[{value, Val}, {tags, Tags} | Post] | Acc]);
do_form_post(_Time, [], Acc) -> jsx:encode(Acc).

