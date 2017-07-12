-module(flow_falcon_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> application:start(flow_falcon) end).
-define(Clearnup, fun(_) -> application:stop(flow_falcon) end).

basic_test_() ->
    {inorder,
     {setup, ?Setup, ?Clearnup,
      [{"falcon",
         fun() ->
                 ?assertEqual(ok, flow_falcon:falcon([{val, a, b, 1}]))
         end},
       {"flow",
         fun() ->
                 flow_falcon:flow_acc(k1, k2, 10),
                 flow_falcon:flow_acc(k1, k2, 10),
                 ?assertEqual([{k2, 20}], proplists:get_value(k1, flow_falcon:flow_list())),
                 flow_falcon:flow_acc(k1, k3, 10),
                 ?assertEqual([{k2, 20}, {k3, 10}], proplists:get_value(k1, flow_falcon:flow_list()))
         end},
       {"flow_falcon",
        {timeout, 70,
         fun() ->
                 Fun = fun F(X) -> receive after 1000 -> io:format(user, "wait second ~p~n", [X]), F(X - 1) end end,
                 spawn(fun() -> Fun(60) end),
                 timer:sleep(61 * 1000),
                 ?assertEqual(2, flow_falcon:falcon_cnt())
         end}
       }
      ]}
    }.

