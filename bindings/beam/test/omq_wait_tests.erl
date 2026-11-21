-module(omq_wait_tests).

-include_lib("eunit/include/eunit.hrl").

endpoint(Name) ->
    Id = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<"inproc://", Name/binary, "-", Id/binary>>.

blocked_receivers_leave_dirty_schedulers_free_test() ->
    {ok, Ctx} = omq:context(),
    Count = erlang:system_info(dirty_io_schedulers) + 2,
    Sockets = [bound(Ctx, pull, <<"idle">>) || _ <- lists:seq(1, Count)],
    Receivers = [spawn(fun() -> omq:recv(Socket) end) || Socket <- Sockets],
    try
        await_waiting(Receivers),
        Probe = bound(Ctx, pull, <<"probe">>),
        ?assertMatch({ok, {error, timeout, _}}, within(2000, fun() -> omq:recv(Probe, 20) end)),
        ?assertMatch({ok, {ok, _}}, within(2000, fun() -> file:read_file(code:which(omq)) end))
    after
        [exit(Receiver, kill) || Receiver <- Receivers],
        ok = omq:term(Ctx)
    end.

killed_receiver_exits_and_leaves_the_message_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"killed-receiver">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    Receiver = spawn(fun() -> omq:recv(Pull) end),
    await_waiting([Receiver]),
    ?assertEqual(killed, kill_and_await_down(Receiver)),
    ok = omq:send(Push, <<"kept">>),
    ?assertEqual({ok, <<"kept">>}, omq:recv(Pull, 1000)),
    ok = omq:term(Ctx).

receivers_wake_on_close_test() ->
    {ok, Ctx} = omq:context(),
    Pull = bound(Ctx, pull, <<"closed-receiver">>),
    Parent = self(),
    Receiver = spawn(fun() -> Parent ! {received, omq:recv(Pull)} end),
    await_waiting([Receiver]),
    ok = omq:close(Pull),
    ?assertMatch({error, closed, _}, await_reply(received)),
    ok = omq:term(Ctx).

receivers_wake_on_context_term_test() ->
    {ok, Ctx} = omq:context(),
    Pull = bound(Ctx, pull, <<"term-receiver">>),
    Parent = self(),
    Receiver = spawn(fun() -> Parent ! {received, omq:recv(Pull)} end),
    await_waiting([Receiver]),
    ok = omq:term(Ctx),
    ?assertMatch({error, closed, _}, await_reply(received)).

open_socket_collected_after_term_test() ->
    {ok, Ctx} = omq:context(),
    Parent = self(),
    Owner = spawn(fun() ->
        _ = bound(Ctx, pull, <<"collected">>),
        Parent ! bound,
        receive stop -> ok end
    end),
    ?assertEqual(bound, await_reply_tag(bound)),
    ok = omq:term(Ctx),
    Ref = monitor(process, Owner),
    Owner ! stop,
    receive {'DOWN', Ref, process, Owner, Reason} -> ?assertEqual(normal, Reason) end,
    erlang:garbage_collect(),
    ?assertMatch({ok, _}, omq:context()).

concurrent_receivers_each_get_one_message_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"shared-receive">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    Parent = self(),
    Receivers = [spawn(fun() -> Parent ! {received, omq:recv(Pull, 2000)} end) || _ <- [1, 2, 3]],
    await_waiting(Receivers),
    [ok = omq:send(Push, Body) || Body <- [<<"a">>, <<"b">>, <<"c">>]],
    Received = lists:sort([await_reply(received) || _ <- Receivers]),
    ?assertEqual([{ok, <<"a">>}, {ok, <<"b">>}, {ok, <<"c">>}], Received),
    ok = omq:term(Ctx).

blocked_send_completes_when_a_peer_connects_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"late-peer">>),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Push, Endpoint),
    Parent = self(),
    Sender = spawn(fun() -> Parent ! {sent, omq:send(Push, <<"late">>)} end),
    await_waiting([Sender]),
    {ok, Pull} = omq:socket(Ctx, pull),
    ok = omq:connect(Pull, Endpoint),
    ?assertEqual(ok, await_reply(sent)),
    ?assertEqual({ok, <<"late">>}, omq:recv(Pull, 1000)),
    ok = omq:term(Ctx).

killed_sender_exits_promptly_test() ->
    {ok, Ctx} = omq:context(),
    Push = bound(Ctx, push, <<"killed-sender">>),
    Sender = spawn(fun() -> omq:send(Push, <<"never">>) end),
    await_waiting([Sender]),
    ?assertEqual(killed, kill_and_await_down(Sender)),
    ok = omq:term(Ctx).

senders_wake_on_close_test() ->
    {ok, Ctx} = omq:context(),
    Push = bound(Ctx, push, <<"closed-sender">>),
    Parent = self(),
    Sender = spawn(fun() -> Parent ! {sent, omq:send(Push, <<"never">>)} end),
    await_waiting([Sender]),
    ok = omq:close(Push),
    ?assertMatch({error, closed, _}, await_reply(sent)),
    ok = omq:term(Ctx).

poll_wakes_when_a_message_arrives_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"poll-wake">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    Parent = self(),
    Poller = spawn(fun() -> Parent ! {polled, omq:poll([{Pull, omq:pollin()}], infinity)} end),
    await_waiting([Poller]),
    ok = omq:send(Push, <<"ready">>),
    ?assertEqual({ok, [{Pull, omq:pollin()}]}, await_reply(polled)),
    ?assertEqual({ok, <<"ready">>}, omq:recv(Pull, 1000)),
    ok = omq:term(Ctx).

poll_on_closed_socket_times_out_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    ok = omq:close(Pull),
    ?assertEqual({ok, []}, omq:poll([{Pull, omq:pollin()}], 20)),
    ok = omq:term(Ctx).

monitor_recv_waits_for_the_next_event_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Monitor} = omq:monitor(Pull),
    ?assertMatch({error, timeout, _}, omq:monitor_recv(Monitor, 10)),
    Parent = self(),
    Waiter = spawn(fun() -> Parent ! {event, omq:monitor_recv(Monitor)} end),
    await_waiting([Waiter]),
    {ok, _} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
    ?assertMatch({ok, #{event := listening}}, await_reply(event)),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

killed_monitor_waiter_returns_the_stream_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Monitor} = omq:monitor(Pull),
    Waiter = spawn(fun() -> omq:monitor_recv(Monitor) end),
    await_waiting([Waiter]),
    ?assertEqual(killed, kill_and_await_down(Waiter)),
    {ok, _} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
    ?assertMatch({ok, #{event := listening}}, omq:monitor_recv(Monitor, 1000)),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

wait_connected_wakes_when_a_peer_connects_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"wait-connected">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ?assertMatch({error, timeout, _}, omq:wait_connected(Pull, 1, 10)),
    Parent = self(),
    Waiter = spawn(fun() -> Parent ! {connected, omq:wait_connected(Pull, 1, infinity)} end),
    await_waiting([Waiter]),
    {ok, Push} = omq:socket(Ctx, push),
    ok = omq:connect(Push, Endpoint),
    ?assertEqual({ok, 1}, await_reply(connected)),
    ok = omq:term(Ctx).

wait_subscribed_wakes_on_a_subscription_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"wait-subscribed">>),
    {ok, Pub} = omq:socket(Ctx, pub),
    {ok, Endpoint} = omq:bind(Pub, Endpoint),
    Parent = self(),
    Waiter = spawn(fun() -> Parent ! {subscribed, omq:wait_subscribed(Pub, 1, infinity)} end),
    await_waiting([Waiter]),
    {ok, Sub} = omq:socket(Ctx, sub),
    ok = omq:connect(Sub, Endpoint),
    ok = omq:subscribe(Sub, <<"a">>),
    ?assertMatch({ok, _}, await_reply(subscribed)),
    ok = omq:term(Ctx).

bound(Ctx, Type, Name) ->
    {ok, Socket} = omq:socket(Ctx, Type),
    {ok, _} = omq:bind(Socket, endpoint(Name)),
    Socket.

%% Wait until each process is parked in receive.
await_waiting(Pids) ->
    await_waiting(Pids, erlang:monotonic_time(millisecond) + 2000).

await_waiting(Pids, Deadline) ->
    Waiting = lists:all(fun(Pid) -> process_info(Pid, status) =:= {status, waiting} end, Pids),
    Now = erlang:monotonic_time(millisecond),
    if
        Waiting -> ok;
        Now >= Deadline -> erlang:error({not_waiting, [process_info(Pid, current_function) || Pid <- Pids]});
        true ->
            receive after 5 -> await_waiting(Pids, Deadline) end
    end.

within(Timeout, Fun) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() -> Parent ! {Ref, Fun()} end),
    receive
        {Ref, Result} -> {ok, Result}
    after Timeout ->
        exit(Pid, kill),
        timeout
    end.

kill_and_await_down(Pid) ->
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, Reason} -> Reason
    after 1000 ->
        still_running
    end.

await_reply_tag(Tag) ->
    receive
        Tag -> Tag
    after 2000 ->
        {no_reply, Tag}
    end.

await_reply(Tag) ->
    receive
        {Tag, Result} -> Result
    after 2000 ->
        {no_reply, Tag}
    end.
