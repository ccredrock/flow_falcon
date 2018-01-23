%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2017 redrock
%%% @doc console flow and to falcon
%%% @end
%%%-------------------------------------------------------------------
-module(flow_falcon).

-export([start/0, stop/0]).

-export([start_link/0]).

-export([add_acc/3,     %% 增加累计次数
         set_acc/3,     %% 设置累计次数
         set_val/3,     %% 设置实时次数
         inc_total/1,   %% +1总计次数
         inc_total/2,   %% +1总计次数
         add_total/2,   %% 增加总计次数
         add_total/3,   %% 增加总计次数
         set_total/2,   %% 设置总计次数
         set_total/3,   %% 设置总计次数
         list_total/0,  %% 总计数据
         list_flow/0,   %% 累计数据
         list_flow/1,   %% 累计数据
         list_near/1,   %% 最近数据
         list_near/0]). %% 最近数据

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

-define(MINUTE,     1 * 60). %% 1分钟

-define(MIN_LEN,       5 * 60 + 1). %% 5分钟
-define(MIN_NAME_LIST, [one_ps, five_ps]).
-define(MIN_LEN_LIST,  [60, 5 * 60]).

-define(SECOND(), erlang:system_time(seconds)).

-type to_string()::atom|binary|integer|list.

-record(state, {start_time = 0,     %% 启动时间
                acc_list = [],      %% 累计类数值列表
                val_list = [],      %% 设置类数值列表
                next_minute = 0}).  %% 分钟定时器

%%------------------------------------------------------------------------------
-spec start() -> 'ok' | {'error', any()}.
start() ->
    application:ensure_all_started(?MODULE).

-spec stop() -> 'ok' | {'error', any()}.
stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_acc(to_string(), to_string(), pos_integer()) -> integer() | {'EXIT', any()}.
add_acc(OP, Type, Inc) ->
    catch ets:update_counter(?ETS_ACC, {OP, Type}, Inc, {{OP, Type}, 0}).

-spec set_acc(to_string(), to_string(), pos_integer()) -> true | {'EXIT', any()}.
set_acc(OP, Type, Val) ->
    catch ets:insert(?ETS_ACC, {{OP, Type}, Val}).

-spec set_val(to_string(), to_string(), pos_integer()) -> integer() | {'EXIT', any()}.
set_val(OP, Type, Val) ->
    catch ets:update_counter(?ETS_VAL, {OP, Type}, [{2, 1}, {3, Val}], {{OP, Type}, 0, 0}).

-spec inc_total(to_string()) -> integer() | {'EXIT', any()}.
inc_total(OP) ->
    add_total(OP, 1).

-spec inc_total(to_string(), to_string()) -> integer() | {'EXIT', any()}.
inc_total(OP, Type) ->
    add_total(OP, Type, 1).

-spec add_total(to_string(), pos_integer()) -> integer() | {'EXIT', any()}.
add_total(OP, Inc) ->
    add_acc(total, OP, Inc).

-spec add_total(to_string(), to_string(), pos_integer()) -> integer() | {'EXIT', any()}.
add_total(OP, Type, Inc) ->
    add_acc(total, OP, Inc),
    add_acc(OP, Type, Inc).

-spec set_total(to_string(), pos_integer()) -> integer() | {'EXIT', any()}.
set_total(OP, Val) ->
    set_val(total, OP, Val).

-spec set_total(to_string(), to_string(), pos_integer()) -> integer() | {'EXIT', any()}.
set_total(OP, Type, Val) ->
    set_val(total, OP, Val),
    set_val(OP, Type, Val).

-spec list_total() -> [tuple()].
list_total() ->
    FL = list_flow(),
    [{last_second, proplists:get_value(last_second, FL, 0)},
     {total, proplists:get_value(total, FL, [])},
     {total, proplists:get_value(total, list_near(), [])}].

-spec list_flow() -> [tuple()].
list_flow() ->
    gen_server:call(?MODULE, flow_list, ?TIMEOUT).

-spec list_flow(to_string()) -> [tuple()].
list_flow(OP) ->
    proplists:get_value(OP, list_flow()).

-spec list_near() -> [tuple()].
list_near() ->
    gen_server:call(?MODULE, flow_near, ?TIMEOUT).

-spec list_near(to_string()) -> [tuple()].
list_near(OP) ->
    proplists:get_value(OP, list_near()).

-spec falcon_cnt() -> pos_integer().
falcon_cnt() ->
    proplists:get_value(falcon_cnt, list_flow(profile)).

%%------------------------------------------------------------------------------
%% @hidden
init([]) ->
    ets:new(?ETS_ACC, [named_table, public, {write_concurrency, true}]),
    ets:new(?ETS_VAL, [named_table, public, {write_concurrency, true}]),
    {ok, #state{start_time = ?SECOND()}, 0}.

%% @hidden
handle_call(flow_list, _From, State) ->
    {reply, catch do_flow_list(State), State};
handle_call(flow_near, _From, State) ->
    {reply, catch do_flow_near(State), State};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

%% @hidden
handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @hidden
terminate(_Reason, _State) ->
    ok.

%% @hidden
handle_info(timeout, State) ->
    State1 = do_second(State),
    State2 = do_minute(State1),
    erlang:send_after(?TIMEOUT, self(), timeout),
    {noreply, State2};

%% @hidden
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @doc timer
%%------------------------------------------------------------------------------
%% @private
do_second(State) ->
    State#state{acc_list = lists:sublist([ets:tab2list(?ETS_ACC) | State#state.acc_list], ?MIN_LEN),
                val_list = lists:sublist([ets:tab2list(?ETS_VAL) | State#state.val_list], ?MIN_LEN)}.

%% @private
do_minute(State) ->
    Now = ?SECOND(),
    case Now >= State#state.next_minute of
        false -> State;
        true ->
            catch do_flow_sys(),
            do_falcon(State),
            State#state{next_minute = Now + ?MINUTE}
    end.

%%------------------------------------------------------------------------------
%% @private
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

%% @private
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
%% @private
do_falcon(State) ->
    case falcon(do_falcon_flow(State)) of
        ok -> add_acc(profile, falcon_cnt, 1);
        {error, Reason} -> error_logger:error_msg("flow_falcon error ~p~n", [{Reason}])
    end.

%%------------------------------------------------------------------------------
%% @private
do_falcon_flow(State) ->
    AccFlow = ets:tab2list(?ETS_ACC),
    AddList = do_sub_flow(AccFlow, do_get_nth_flow(?MINUTE + 1, State#state.acc_list)),
    ValFlow = ets:tab2list(?ETS_VAL),
    ValFlow1 = do_sub_flow(ValFlow, do_get_nth_flow(?MINUTE + 1, State#state.val_list)),
    ValList = [{Key, Val div max(1, Count)} || {Key, Count, Val} <- ValFlow1],
    [{val, A, B, C} || {{A, B}, C} <- ValList]
    ++ [{acc, A, B, C} || {{A, B}, C} <- AccFlow]
    ++ [{add, A, B, C} || {{A, B}, C} <- AddList].

%% @private
do_sub_flow(NewList, []) -> NewList;
do_sub_flow(NewList, OldList) ->
    Fun = fun({Key, Count}, Acc) ->
                  case lists:keyfind(Key, 1, OldList) of
                      false -> lists:keystore(Key, 1, Acc, {Key, Count});
                      {_, Count1} -> lists:keystore(Key, 1, Acc, {Key, Count - Count1})
                  end;
             ({Key, Count, Val}, Acc) ->
                  case lists:keyfind(Key, 1, OldList) of
                      false -> lists:keystore(Key, 1, Acc, {Key, Count, Val});
                      {_, Count1, Val1} -> lists:keystore(Key, 1, Acc, {Key, Count - Count1, Val - Val1})
                  end
          end,
    lists:foldl(Fun, [], NewList).

%% @private
do_get_nth_flow(Nth, List) ->
    case length(List) < Nth of
        false -> lists:nth(Nth, List);
        true -> []
    end.

%%------------------------------------------------------------------------------
%% @private
do_flow_list(State) ->
    catch do_flow_sys(),
    List = ets:tab2list(?ETS_VAL) ++ ets:tab2list(?ETS_ACC),
    [{last_second, do_format_time(do_get_last(State))}] ++ do_format_flow(List).

do_get_last(State) -> ?SECOND() - State#state.start_time.

%% @private [{{OP, Type}, Count}]
do_format_flow(Flow) ->
    Fun = fun(E, Acc) ->
                  {OP, Type} = element(1, E),
                  T = {Type, do_format_vals(erlang:delete_element(1, E))},
                  case lists:keyfind(OP, 1, Acc) of
                      false -> lists:keystore(OP, 1, Acc, {OP, [T]});
                      {_, List} -> lists:keyreplace(OP, 1, Acc, {OP, lists:sort([T | List])})
                  end
          end,
    lists:sort(lists:foldl(Fun, [], Flow)).

%% @private
do_format_vals({V1}) -> do_format_number(V1);
do_format_vals({V1, V2}) -> do_format_number(V1) ++ " => " ++ do_format_number(V2);
do_format_vals(Tuple) ->
    Fun = fun(V, "") -> do_format_number(V);
             (V, Str) -> Str ++ ", " ++ do_format_number(V)
          end,
    lists:foldl(Fun, "", tuple_to_list(Tuple)).

%% @private
do_format_number(V) ->
    if
        V =< 1024 -> integer_to_list(V);
        V =< 1024 * 1024 -> float_to_list(V / 1024, [{decimals, 3}]) ++ "K";
        V =< 1024 * 1024 * 1024 -> float_to_list(V / 1024 / 1024, [{decimals, 3}]) ++ "M";
        true -> float_to_list(V / 1024 / 1024 / 1024, [{decimals, 3}]) ++ "G"
    end.

%% @private
do_format_time(V) ->
    if
        V =< 60 -> integer_to_list(V) ++ "s";
        V =< 60 * 60 -> integer_to_list(V div 60) ++ "m" ++ do_format_time(V rem 60);
        true -> integer_to_list(V div 3600) ++ "h" ++ do_format_time(V rem 3600)
    end.

%% @private
do_flow_near(State) ->
    Last = do_get_last(State),
    AccFlow = ets:tab2list(?ETS_ACC),
    AccList = State#state.acc_list,
    AccResult = do_list_flow(?MIN_LEN_LIST, AccList, AccFlow, length(AccList), do_ps_flow(AccFlow, Last), []),
    ValFlow = ets:tab2list(?ETS_VAL),
    ValList = State#state.val_list,
    ValResult = do_list_flow(?MIN_LEN_LIST, ValList, ValFlow, length(ValList), do_ps_flow(ValFlow, Last), []),
    [{last_second, do_format_time(Last)},
     {op, [list_to_tuple([type] ++ ?MIN_NAME_LIST ++ [acc_ps])]}
     | do_format_flow(AccResult ++ ValResult)].

%% @private
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

%% @private per second
do_ps_flow(List, Time) ->
    [{{OP, Type}, Count div Time} || {{OP, Type}, Count} <- List]
    ++ [{{OP, Type}, Val div max(1, Count)} || {{OP, Type}, Count, Val} <- List].

%% @private List:{a,1,2}, New:{a,3} Acc:{a,1,2,3}
do_append_flow([], New) -> New;
do_append_flow(List, []) -> List;
do_append_flow(List, New) ->
    Fun = fun({Key, Count}, Acc) ->
                  E = lists:keyfind(Key, 1, List),
                  [erlang:insert_element(erlang:tuple_size(E) + 1, E, Count) | Acc]
          end,
    lists:foldl(Fun, [], New).

%%------------------------------------------------------------------------------
-spec falcon([{Way::acc|add|val, OP::to_string(), Type::to_string(), Val::pos_integer()}]) -> ok | skip | {error, any()}.
falcon([]) -> ok;
falcon(List) ->
    case application:get_env(flow_falcon, falcon) of
        undefined -> skip;
        {ok, Props} ->
            {ok, HostName} = inet:gethostname(),
            Post = [{metric, list_to_binary(proplists:get_value(metric, Props))},
                    {endpoint, list_to_binary(proplists:get_value(endpoint, Props, HostName))},
                    {timestamp, ?SECOND()},
                    {counterType, 'GAUGE'},
                    {step, ?MINUTE}],
            Json = do_form_post(Post, List, []),
            case catch httpc:request(post, {proplists:get_value(url, Props), [], "application/json", Json}, [], []) of
                {ok, _} -> ok;
                {error, Reason} -> {error, Reason};
                Reason -> {error, Reason}
            end
    end.

%% @private
do_form_post(Post, [{Way, OP, Type, Val} | T], Acc) ->
    Tags = iolist_to_binary(["way=", to_list(Way),
                             ",op=", to_list(OP),
                             ",type=", to_list(Type)]),
    do_form_post(Post, T, [[{value, Val}, {tags, Tags} | Post] | Acc]);
do_form_post(_Time, [], Acc) -> jsx:encode(Acc).

%% @private
to_list(X) when is_atom(X) -> atom_to_list(X);
to_list(X) when is_binary(X) -> binary_to_list(X);
to_list(X) when is_integer(X) -> integer_to_list(X);
to_list(X) when is_list(X) -> X.

