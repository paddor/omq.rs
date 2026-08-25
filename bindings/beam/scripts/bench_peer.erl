#!/usr/bin/env escript
%%! -noshell

main([Bench, Impl, Role, Endpoint, SizeText, DurationText, WarmupText]) ->
    Root = filename:dirname(filename:dirname(escript:script_name())),
    add_paths(Root),
    Size = list_to_integer(SizeText),
    DurationMs = seconds_to_ms(DurationText),
    WarmupMs = seconds_to_ms(WarmupText),
    Payload = binary:copy(<<"x">>, Size),
    case {Bench, Role} of
        {"pushpull", "pull"} -> pull(Impl, Endpoint, Payload, DurationMs, WarmupMs);
        {"pushpull", "push"} -> push(Impl, Endpoint, Payload);
        {"reqrep", "rep"} -> rep(Impl, Endpoint);
        {"reqrep", "req"} -> req(Impl, Endpoint, Payload, DurationMs, WarmupMs);
        _ -> die("bad benchmark args")
    end;
main(_) ->
    die("usage: bench_peer.erl <pushpull|reqrep> <omq-erlang|omq-gleam|erlzmq> <push|pull|req|rep> <endpoint> <size> <duration> <warmup>").

add_paths(Root) ->
    Paths = [
        ["_build", "default", "lib", "omq", "ebin"],
        ["_build", "test", "lib", "omq", "ebin"],
        ["gleam", "build", "dev", "erlang", "omq_gleam", "ebin"],
        ["gleam", "build", "dev", "erlang", "gleam_stdlib", "ebin"]
    ],
    [code:add_patha(filename:join([Root | Parts])) || Parts <- Paths],
    ok.

seconds_to_ms(Text) ->
    round(list_to_float(Text) * 1000).

now_ms() ->
    erlang:monotonic_time(millisecond).

deadline(Ms) ->
    now_ms() + Ms.

open("omq-erlang", Type) ->
    {ok, Ctx} = omq:context(),
    {ok, Sock} = omq:socket(Ctx, Type),
    {omq_erlang, Ctx, Sock};
open("omq-gleam", Type) ->
    {ok, Ctx} = omq_gleam:context(),
    {ok, Sock} = omq_gleam:socket(Ctx, type_code(Type)),
    {omq_gleam, Ctx, Sock};
open("erlzmq", Type) ->
    case code:ensure_loaded(erlzmq) of
        {module, erlzmq} ->
            {ok, Ctx} = erlzmq:context(),
            {ok, Sock} = erlzmq:socket(Ctx, Type),
            _ = erlzmq:setsockopt(Sock, linger, 0),
            {erlzmq, Ctx, Sock};
        _ ->
            die("erlzmq unavailable")
    end.

type_code(pair) -> 0;
type_code(pub) -> 1;
type_code(sub) -> 2;
type_code(req) -> 3;
type_code(rep) -> 4;
type_code(dealer) -> 5;
type_code(router) -> 6;
type_code(pull) -> 7;
type_code(push) -> 8.

bind({omq_erlang, _, Sock}, Endpoint) -> omq:bind(Sock, Endpoint);
bind({omq_gleam, _, Sock}, Endpoint) -> omq_gleam:bind(Sock, list_to_binary(Endpoint));
bind({erlzmq, _, Sock}, Endpoint) -> erlzmq:bind(Sock, Endpoint).

connect({omq_erlang, _, Sock}, Endpoint) -> omq:connect(Sock, Endpoint);
connect({omq_gleam, _, Sock}, Endpoint) -> omq_gleam:connect(Sock, list_to_binary(Endpoint));
connect({erlzmq, _, Sock}, Endpoint) -> erlzmq:connect(Sock, Endpoint).

send({omq_erlang, _, Sock}, Msg) -> omq:send(Sock, Msg);
send({omq_gleam, _, Sock}, Msg) -> omq_gleam:send(Sock, Msg);
send({erlzmq, _, Sock}, Msg) -> erlzmq:send(Sock, Msg).

send_fast({omq_erlang, _, Sock}, Msg) -> omq:try_send(Sock, Msg);
send_fast({omq_gleam, _, Sock}, Msg) -> omq:try_send(Sock, Msg);
send_fast(Sock, Msg) -> send(Sock, Msg).

recv({omq_erlang, _, Sock}) -> omq:recv(Sock);
recv({omq_gleam, _, Sock}) -> omq_gleam:recv(Sock);
recv({erlzmq, _, Sock}) -> erlzmq:recv(Sock).

try_recv({omq_erlang, _, Sock}) -> omq:try_recv(Sock);
try_recv({omq_gleam, _, Sock}) -> normalize_gleam(omq_gleam:try_recv(Sock));
try_recv(Sock) -> recv(Sock).

normalize_gleam({error, {<<"would_block">>, Reason}}) ->
    {error, would_block, Reason};
normalize_gleam(Result) ->
    Result.

close({omq_erlang, Ctx, Sock}) -> omq:close(Sock), omq:term(Ctx);
close({omq_gleam, _Ctx, Sock}) -> omq_gleam:close(Sock);
close({erlzmq, Ctx, Sock}) -> erlzmq:close(Sock), erlzmq:term(Ctx).

pull(Impl, Endpoint, Payload, DurationMs, WarmupMs) ->
    Sock = open(Impl, pull),
    okish(bind(Sock, Endpoint)),
    io:format("READY ~s~n", [Endpoint]),
    drain_until(Sock, byte_size(Payload), deadline(WarmupMs)),
    Start = now_ms(),
    Count = drain_until(Sock, byte_size(Payload), Start + DurationMs),
    End = now_ms(),
    close(Sock),
    result(Impl, "throughput", byte_size(Payload), Count, (End - Start) / 1000).

push(Impl, Endpoint, Payload) ->
    Sock = open(Impl, push),
    okish(connect(Sock, Endpoint)),
    push_loop(Sock, Payload).

rep(Impl, Endpoint) ->
    Sock = open(Impl, rep),
    okish(bind(Sock, Endpoint)),
    io:format("READY ~s~n", [Endpoint]),
    rep_loop(Sock).

req(Impl, Endpoint, Payload, DurationMs, WarmupMs) ->
    Sock = open(Impl, req),
    okish(connect(Sock, Endpoint)),
    req_until(Sock, Payload, deadline(WarmupMs), []),
    Start = now_ms(),
    Samples = req_until(Sock, Payload, Start + DurationMs, []),
    End = now_ms(),
    Count = length(Samples),
    close(Sock),
    latency_result(Impl, byte_size(Payload), Count, (End - Start) / 1000, Samples).

drain_until(Sock, Size, Deadline) ->
    Interval = recv_timer_check_interval(Size),
    drain_until(Sock, Size, Deadline, 0, Interval, Interval).

drain_until(Sock, Size, Deadline, Count, 0, Interval) ->
    case now_ms() >= Deadline of
        true -> Count;
        false -> drain_until(Sock, Size, Deadline, Count, Interval, Interval)
    end;
drain_until(Sock, Size, Deadline, Count, ChecksLeft, Interval) ->
    case try_recv(Sock) of
        {ok, Msg} ->
            true = byte_size(Msg) == Size,
            drain_until(Sock, Size, Deadline, Count + 1, ChecksLeft - 1, Interval);
        {error, would_block, _} ->
            case now_ms() >= Deadline of
                true -> Count;
                false ->
                    erlang:yield(),
                    drain_until(Sock, Size, Deadline, Count, ChecksLeft, Interval)
            end
    end.

recv_timer_check_interval(Size) ->
    case Size =< 1024 of
        true -> 4096;
        false -> 256
    end.

push_loop(Sock, Payload) ->
    case send_fast(Sock, Payload) of
        ok ->
            push_loop(Sock, Payload);
        {error, would_block, _} ->
            erlang:yield(),
            push_loop(Sock, Payload);
        Error ->
            okish(Error),
            push_loop(Sock, Payload)
    end.

rep_loop(Sock) ->
    {ok, Msg} = recv(Sock),
    okish(send(Sock, Msg)),
    rep_loop(Sock).

req_until(Sock, Payload, Deadline, Samples) ->
    case now_ms() >= Deadline of
        true -> Samples;
        false ->
            Start = erlang:monotonic_time(microsecond),
            okish(send(Sock, Payload)),
            {ok, Payload} = recv(Sock),
            End = erlang:monotonic_time(microsecond),
            req_until(Sock, Payload, Deadline, [End - Start | Samples])
    end.

okish(ok) -> ok;
okish({ok, _}) -> ok;
okish({error, Reason}) -> die(io_lib:format("~p", [Reason]));
okish({error, Class, Reason}) -> die(io_lib:format("~p: ~p", [Class, Reason])).

result(Impl, Kind, Size, Count, Seconds) ->
    MsgsS = Count / Seconds,
    GBS = MsgsS * Size / 1000000000,
    io:format(
        "RESULT {\"impl\":\"~s\",\"kind\":\"~s\",\"msg_size\":~B,\"messages\":~B,\"seconds\":~.6f,\"msgs_s\":~.3f,\"gb_s\":~.6f}~n",
        [Impl, Kind, Size, Count, Seconds, MsgsS, GBS]
    ).

latency_result(Impl, Size, Count, Seconds, Samples) ->
    Sorted = lists:sort(Samples),
    P50 = percentile(Sorted, 0.50),
    P99 = percentile(Sorted, 0.99),
    io:format(
        "RESULT {\"impl\":\"~s\",\"kind\":\"latency\",\"msg_size\":~B,\"messages\":~B,\"seconds\":~.6f,\"p50_us\":~.3f,\"p99_us\":~.3f}~n",
        [Impl, Size, Count, Seconds, P50 * 1.0, P99 * 1.0]
    ).

percentile([], _) -> 0;
percentile(Sorted, P) ->
    Index = min(length(Sorted), max(1, ceil(length(Sorted) * P))),
    lists:nth(Index, Sorted).

die(Message) ->
    io:format(standard_error, "~s~n", [Message]),
    halt(1).
