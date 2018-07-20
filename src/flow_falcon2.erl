%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2017 redrock
%%% @doc console flow and to falcon
%%% @end
%%%-------------------------------------------------------------------
-module(flow_falcon2).

-export([add_count/2,     %% 增加次数
         add_count/3,     %% 增加次数
         set_count/3,     %% 设置次数
         add_value/3,     %% 设置数值
         list_count/0,    %% 可视次数
         list_count/1,    %% 详细次数
         list_value/0,    %% 可视数值
         list_value/1,    %% 详细数值
         list_metric/1,   %% 可视指标
         list_metric/2]). %% 详细指标

%% callbacks
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-import(flow_util, [format_number/1]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-type to_string()::atom | binary | integer | list.

-define(PRFILE_TIMEOUT, 1000 * 10).
-define(LOOP_TIMEOUT,   1000).
-define(FALCON_TIMEOUT, 1000).

-define(MINUTE_IN_SECOND,  1 * 60). %% 1分钟

-define(CACHE_DATA_TIME(), (application:get_env(flow_falcon, cache_data_time, 5 * 60) + 1)). %% 5分钟
-define(FALCON_CONFIG(), application:get_env(flow_falcon, falcon, [])). %%

-define(DEFAULT_METRIC, profile).
-define(METRIC_NAME(X), binary_to_atom(<<"flow_falcon_", (atom_to_binary(X, utf8))/binary>>, utf8)).

-define(ARGS_HUMAN(X), re:run(X, "h") =/= nomatch).
-define(ARGS_LEVEL(X), min(3, catch binary_to_integer(re:replace(X, ".*([0-3]).*", "\\1", [{return, binary}])))).
-define(METRIC_LIST(), lists:sort([X || {X, _, _, _} <- supervisor:which_children(flow_falcon_sup), X =/= flow_falcon])).

-record(state, {start_time  = 0,    %% 启动时间
                metric      = null, %%
                ets         = null, %%
                count_album = [],   %% [#{tags => val}]
                value_album = [],   %% [#{tags => #{min => v1, max => v2, cnt => v3, acc => v4}}]
                value_photo = #{},  %% #{tags => #{min => v1, max => v2, cnt => v3, acc => v4}}
                value_total = #{},  %% #{tags => #{min => v1, max => v2, cnt => v3, acc => v4}}
                next_minute = 0}).  %% 分钟定时器

%%------------------------------------------------------------------------------
%% @doc put val
%%------------------------------------------------------------------------------
-spec add_count(atom(), [to_string()], pos_integer()) -> any().
add_count(Metric, Tag) when is_atom(Tag) -> add_count(Metric, [Tag], 1);
add_count(Metric, Tags) -> add_count(Metric, Tags, 1).
add_count(Metric, Tags, Count) ->
    ETS = ?METRIC_NAME(Metric),
    case catch ets:update_counter(ETS, Tags, Count, {Tags, 0}) of
        {'EXIT', _} ->
            flow_falcon_sup:start_child(Metric),
            catch ets:update_counter(ETS, Tags, Count, {Tags, 0});
        Result ->
            Result
    end.

-spec set_count(atom(), [to_string()], pos_integer()) -> any().
set_count(Metric, Tags, Count) ->
    ETS = ?METRIC_NAME(Metric),
    case catch ets:insert(ETS, {Tags, Count}) of
        {'EXIT', _} ->
            flow_falcon_sup:start_child(Metric),
            catch ets:insert(ETS, {Tags, Count});
        Result ->
            Result
    end.

%%------------------------------------------------------------------------------
-spec add_value(atom(), [to_string()], pos_integer()) -> any().
add_value(Metric, Tags, Val) ->
    Name = ?METRIC_NAME(Metric),
    case whereis(Name) of
        undefined ->
            flow_falcon_sup:start_child(Metric),
            gen_server:cast(Name, {add_value, Tags, Val});
        Proc ->
            gen_server:cast(Proc, {add_value, Tags, Val})
    end.

%%------------------------------------------------------------------------------
-spec list_count() -> [].
list_count() ->
    list_count("0").

-spec list_count(string()) -> [].
list_count(Args) ->
    Human = ?ARGS_HUMAN(Args),
    Level = ?ARGS_LEVEL(Args),
    List = ?METRIC_LIST(),
    State = sys:get_state(?METRIC_NAME(?DEFAULT_METRIC)),
    Time = erlang:system_time(seconds) - State#state.start_time,
    [{last, frm_time(Human, Time)}] ++ lists:flatten([human_count(M, Time, Human, Level) || M <- List]).

%% @private
human_count(Metric, Time, Human, Level) ->
    Name = ?METRIC_NAME(Metric),
    case catch sys:get_state(?METRIC_NAME(Metric)) of
        {'EXIT', _} -> [];
        S ->
            case human_count(Level, S#state.count_album, ets:tab2list(Name), Human, Time) of
                [] -> [];
                L -> {Metric, L}
            end
    end.

%% @private
human_count(1, [_ | _] = List, _Total, Human, Time) ->
    Head = hd(List),
    Mid  = nth_list(?MINUTE_IN_SECOND, List),
    frm_count(Human, fold_count(sub_photo(Head, Mid)), min(?MINUTE_IN_SECOND, Time));
human_count(2, [_ | _] = List, _Total, Human, Time) ->
    Head = hd(List),
    Tail = nth_list(TailNth = max(?MINUTE_IN_SECOND, length(List)), List),
    frm_count(Human, fold_count(sub_photo(Head, Tail)), min(TailNth, Time));
human_count(3, _List, Total, Human, Time) ->
    frm_count(Human, fold_count(Total), Time);
human_count(0, _List, Total, Human, _Time) ->
    frm_count(Human, fold_count(Total), 1);
human_count(_Level, _List, _Total, _Human, _Time) -> [].

%% @private
frm_count(false, L, D) -> [{frm_tag(K), V div D} || {K, V} <- L];
frm_count(true, L, D) -> [{frm_tag(K), format_number(V div D)} || {K, V} <- L].

%% @private
frm_time(false, V) -> V;
frm_time(true, V) -> flow_util:format_time(V).

%% @private
frm_tag([K]) -> K;
frm_tag(K) -> K.

%%------------------------------------------------------------------------------
-spec list_value() -> [].
list_value() -> list_value("0").

-spec list_value(string()) -> [].
list_value(Args) ->
    Human = ?ARGS_HUMAN(Args),
    Level = ?ARGS_LEVEL(Args),
    List = ?METRIC_LIST(),
    State = sys:get_state(?METRIC_NAME(?DEFAULT_METRIC)),
    Time = erlang:system_time(seconds) - State#state.start_time,
    [{last, frm_time(Human, Time)}] ++ lists:flatten([human_value(M, Time, Human, Level) || M <- List]).

%% @private
human_value(Metric, Time, Human, Level) ->
    case catch sys:get_state(?METRIC_NAME(Metric)) of
        {'EXIT', _} -> [];
        S ->
            case human_value(Level, S#state.value_album, maps:to_list(S#state.value_total), Human, Time) of
                [] -> [];
                L -> {Metric, L}
            end
    end.

%% @private
human_value(1, [_ | _] = List, _Total, Human, Time) ->
    Near = len_list(min(?MINUTE_IN_SECOND, Time), List),
    Fun = fun({Tags, Value}, Acc) -> put_value(Tags, Value, Acc) end,
    Fun1 = fun(New, Acc) -> lists:foldl(Fun, Acc, maps:to_list(New)) end,
    frm_value(Human, fold_value(maps:to_list(lists:foldl(Fun1, #{}, Near))));
human_value(2, [_ | _] = List, _Total, Human, _Time) ->
    Long = len_list(max(?MINUTE_IN_SECOND, length(List)), List),
    Fun = fun({Tags, Value}, Acc) -> put_value(Tags, Value, Acc) end,
    Fun1 = fun(New, Acc) -> lists:foldl(Fun, Acc, maps:to_list(New)) end,
    frm_value(Human, fold_value(maps:to_list(lists:foldl(Fun1, #{}, Long))));
human_value(3, _List, Total, Human, _Time) ->
    frm_value(Human, fold_value(Total));
human_value(_Level, _List, _Total, _Human, _Time) -> [].

%% @private
frm_value(false, L) ->
    [{frm_tag(K), [{cnt, Cnt}, {avg, Avg}, {max, Max}, {min, Min}]}
     || {K, #{cnt := Cnt, avg := Avg, max := Max, min := Min}} <- L];
frm_value(true, L) ->
    [{frm_tag(K), [{cnt, format_number(Cnt)},
                   {avg, format_number(Avg)},
                   {max, format_number(Max)},
                   {min, format_number(Min)}]}
     || {K, #{cnt := Cnt, avg := Avg, max := Max, min := Min}} <- L].

%%------------------------------------------------------------------------------
list_metric(Metric) ->
    list_metric(Metric, "0").

list_metric(Metric, Args) ->
    Human = ?ARGS_HUMAN(Args),
    Level = ?ARGS_LEVEL(Args),
    State = sys:get_state(?METRIC_NAME(?DEFAULT_METRIC)),
    Time = erlang:system_time(seconds) - State#state.start_time,
    [{last, frm_time(Human, Time)}]
    ++ lists:flatten([human_count(Metric, Time, Human, Level)])
    ++ lists:flatten([human_value(Metric, Time, Human, Level)]).

%%------------------------------------------------------------------------------
%% @doc gen_server
%%------------------------------------------------------------------------------
-spec start_link(atom()) -> {ok, pid()} | {error, any()}.
start_link(Metric) ->
    gen_server:start_link({local, ?METRIC_NAME(Metric)}, ?MODULE, [Metric], []).

%% @hidden
init([Metric]) ->
    ETS = ets:new(?METRIC_NAME(Metric), [named_table, public, {write_concurrency, true}]),
    Metric =:= ?DEFAULT_METRIC andalso erlang:send_after(?PRFILE_TIMEOUT, self(), profile),
    {ok, #state{metric = Metric, ets = ETS, start_time = erlang:system_time(seconds)}, 0}.

%% @hidden
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

%% @hidden
handle_cast({add_value, Tags, Value}, State) ->
    {noreply, handle_add_value(Tags, Value, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
handle_info(profile, State) ->
    catch handle_profile(),
    erlang:send_after(?PRFILE_TIMEOUT, self(), profile),
    {noreply, State};
handle_info(timeout, State) ->
    State1 = handle_timer(State),
    erlang:send_after(?LOOP_TIMEOUT, self(), timeout),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @hidden
terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% @doc add_value
%%------------------------------------------------------------------------------
handle_add_value(Tags, Value, #state{value_photo = Photo} = State) ->
    Value1 = #{cnt => 1, acc => Value, min => Value, max => Value},
    State#state{value_photo = put_value(Tags, Value1, Photo)}.

%% @private
put_value(Key, #{cnt := Cnt, acc := Acc, min := Min, max := Max}, Map) ->
    Val = maps:get(Key, Map, #{}),
    Cnt1 = maps:get(cnt, Val, 0) + Cnt,
    Acc1 = maps:get(acc, Val, 0) + Acc,
    Val1 = #{cnt => Cnt1,
             acc => Acc1,
             avg => Acc1 div Cnt1,
             min => min(Min, maps:get(min, Val, Min)),
             max => max(Max, maps:get(max, Val, Max))},
    maps:put(Key, Val1, Map).

%%------------------------------------------------------------------------------
%% @doc profile
%%------------------------------------------------------------------------------
handle_profile() ->
    add_value(profile, [cpu], flow_util:cal_cpu()),
    add_value(profile, [total_memory], erlang:memory(total)),
    add_value(profile, [process_memory], erlang:memory(processes)),
    add_value(profile, [binary_memory], erlang:memory(binary)),
    {{_, Input}, {_, Output}} = statistics(io),
    set_count(profile, [io_input], Input),
    set_count(profile, [io_output], Output),
    add_value(profile, [process_count], erlang:system_info(process_count)),
    MsgQ = lists:max([Y || {_, Y} <- [process_info(X, message_queue_len) || X <- processes()]]),
    add_value(profile, [msg_queue], MsgQ).

%%------------------------------------------------------------------------------
%% @doc timer
%%------------------------------------------------------------------------------
%% @private
handle_timer(#state{ets = ETS} = State) ->
    State1 = try_falcon(State),
    CountPhoto = maps:from_list(ets:tab2list(ETS)),
    cache_data(State1, CountPhoto).

%%------------------------------------------------------------------------------
%% @private
try_falcon(#state{metric = Metric} = State) ->
    case (Now = erlang:system_time(seconds)) >= State#state.next_minute of
        false -> State;
        true ->
            spawn(fun() -> catch falcon_count(Metric, State) end),
            spawn(fun() -> catch falcon_value(Metric, State) end),
            State#state{next_minute = Now + ?MINUTE_IN_SECOND}
    end.

%% @private
falcon_count(Metric, #state{count_album = List}) ->
    AccList = maps:to_list(Acc = nth_list(1, List)),
    Acc1 = [{[acc | K], V} || {K, V} <- fold_count(AccList)],
    Add1 = [{[add | K], V} || {K, V} <- fold_count(sub_photo(Acc, nth_list(?MINUTE_IN_SECOND, List)))],
    catch push_falcon(Metric, Acc1),
    catch push_falcon(Metric, Add1).

%% @private
fold_count(List) ->
    List1 = [[{begin {L, _} = lists:split(X, Tags), L end, Val} || X <- lists:seq(1, length(Tags))] || {Tags, Val} <- List],
    Fun = fun({K, V}, Acc) -> maps:put(K, maps:get(K, Acc, 0) + V, Acc) end,
    lists:sort(maps:to_list(lists:foldl(Fun, #{}, lists:flatten(List1)))).

%% @private
falcon_value(Metric, #state{value_album = List}) ->
    Album = len_list(?MINUTE_IN_SECOND, List),
    Fun = fun({Tags, #{cnt := Cnt, acc := Acc, min := Min, max := Max}},
              #{min := MinMap, max := MaxMap, avg := AvgMap, cnt := CntMap, acc := AccMap}) ->
                  OldCnt = maps:get(Tags, CntMap, 0),
                  OldAcc = maps:get(Tags, AccMap, 0),
                  #{min => maps:put(Tags, min(Min, maps:get(Tags, MinMap, Min)), MinMap),
                    max => maps:put(Tags, max(Max, maps:get(Tags, MaxMap, Max)), MaxMap),
                    cnt => maps:put(Tags, OldCnt + Cnt, CntMap),
                    acc => maps:put(Tags, OldAcc + Acc, AccMap),
                    avg => maps:put(Tags, (OldAcc + Acc) div (OldCnt + Cnt), AvgMap)}
          end,
    Fun1 = fun(MetricData, Acc) -> lists:foldl(Fun, Acc, fold_value(maps:to_list(MetricData))) end,
    Map = lists:foldl(Fun1, #{min => #{}, max => #{}, avg => #{}, cnt => #{}, acc => #{}}, Album),
    catch push_falcon(Metric, [{[min | K], V} || {K, V} <- maps:to_list(maps:get(min, Map))]),
    catch push_falcon(Metric, [{[max | K], V} || {K, V} <- maps:to_list(maps:get(max, Map))]),
    catch push_falcon(Metric, [{[avg | K], V} || {K, V} <- maps:to_list(maps:get(avg, Map))]).

%% @private
fold_value(List) ->
    List1 = [[{begin {L, _} = lists:split(X, Tags), L end, Val} || X <- lists:seq(1, length(Tags))] || {Tags, Val} <- List],
    Fun = fun({Tags, Val}, Acc) -> put_value(Tags, Val, Acc) end,
    maps:to_list(lists:foldl(Fun, #{}, lists:flatten(List1))).

%% @private
push_falcon(_Metric, []) -> ok;
push_falcon(Metric, List) ->
    case proplists:get_value(url, Props = ?FALCON_CONFIG()) of
        undefined -> ok;
        Url ->
            {ok, HostName} = inet:gethostname(),
            Post = [{metric, list_to_binary(flow_util:to_list(Metric))},
                    {endpoint, list_to_binary(proplists:get_value(endpoint, Props, HostName))},
                    {timestamp, erlang:system_time(seconds)},
                    {counterType, 'GAUGE'},
                    {step, ?MINUTE_IN_SECOND}],
            Json = jsx:encode([[{value, Val}, {tags, form_falcon_tags(KeyList, 0, [])} | Post] || {KeyList, Val} <- List]),
            case catch httpc:request(post,
                                     {Url, [], "application/json", Json},
                                     [{connect_timeout, ?FALCON_TIMEOUT},
                                      {timeout, ?FALCON_TIMEOUT}],
                                     []) of
                {ok, {{_, 200, _}, _, _}} ->
                    add_count(profile, [falcon_cnt, sucess], 1);
                Reason ->
                    add_count(profile, [falcon_cnt, fail], 1),
                    error_logger:error_msg("falcon fail ~p~n", [Reason])
            end
    end.

%% <<"k0=acc,k1=profile,k2=cpu">>
%% @private
form_falcon_tags([K | T], Index, Acc) ->
    Acc1 = [[",", "k", integer_to_binary(Index), "=", flow_util:to_list(K)] | Acc],
    form_falcon_tags(T, Index + 1, Acc1);
form_falcon_tags([], _, Acc) ->
    [H | T] = lists:reverse(Acc),
    iolist_to_binary([tl(H) | T]).

%% @private
sub_photo(New, Old) ->
    case maps:size(Old) of
        0 -> maps:to_list(New);
        _ -> [{K, V - maps:get(K, Old, 0)} || {K, V} <- maps:to_list(New)]
    end.

%% @private
nth_list(Nth, List) when length(List) < Nth -> #{};
nth_list(Nth, List) -> lists:nth(Nth, List).

%% @private
len_list(Len, List) when length(List) < Len -> List;
len_list(Len, List) -> {L, _} = lists:split(Len, List), L.

%%------------------------------------------------------------------------------
%% @private
cache_data(#state{count_album = CountAlbum,
                  value_album = ValueAlbum,
                  value_photo = ValuePhoto,
                  value_total = ValueTotal} = State, CountPhoto) ->
    CountAlbum1 = lists:sublist([CountPhoto | CountAlbum], ?CACHE_DATA_TIME()),
    ValueAlbum1 = lists:sublist([ValuePhoto | ValueAlbum], ?CACHE_DATA_TIME()),
    State#state{count_album = CountAlbum1,
                value_album = ValueAlbum1,
                value_photo = #{},
                value_total = merge_photo(ValuePhoto, ValueTotal)}.

%% @private
merge_photo(Photo, Total) ->
    Fun = fun({Tags, Value}, Acc) -> put_value(Tags, Value, Acc) end,
    lists:foldl(Fun, Total, maps:to_list(Photo)).

