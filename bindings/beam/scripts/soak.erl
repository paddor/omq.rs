#!/usr/bin/env escript
%%! -pa _build/default/lib/omq/ebin

-include_lib("kernel/include/file.hrl").

main(Args) ->
    code:add_patha("_build/default/lib/omq/ebin"),
    ensure_priv(),
    DurationMs = duration_ms(Args),
    io:format("OMQ.beam soak: ~B ms~n", [DurationMs]),
    push_pull(DurationMs),
    req_rep(DurationMs),
    peer_churn(DurationMs),
    ok.

duration_ms([Seconds]) ->
    list_to_integer(Seconds) * 1000;
duration_ms([]) ->
    case os:getenv("OMQ_BEAM_SOAK_SECONDS") of
        false -> 5000;
        Value -> list_to_integer(Value) * 1000
    end.

ensure_priv() ->
    Source = nif_source(),
    Target = "_build/default/lib/omq/priv/omq_beam_native.so",
    ok = filelib:ensure_dir(Target),
    case file:copy(Source, Target) of
        {ok, _Bytes} -> ok;
        {error, same} -> ok;
        {error, Reason} -> erlang:error({nif_stage_failed, Reason})
    end.

nif_source() ->
    Candidates = [
        "native/target/debug/libomq_beam_native.so",
        "native/target/release/libomq_beam_native.so",
        "priv/omq_beam_native.so"
    ],
    case [Path || Path <- Candidates, usable_file(Path)] of
        [Path | _] -> Path;
        [] -> erlang:error(no_usable_nif_shared_object)
    end.

usable_file(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = regular, size = Size}} when Size > 0 -> true;
        _ -> false
    end.

push_pull(DurationMs) ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    ok = omq:setsockopt(Push, sndtimeo, 100),
    ok = omq:setsockopt(Pull, rcvtimeo, 100),
    {ok, Endpoint} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
    ok = omq:connect(Push, Endpoint),
    Parent = self(),
    Sender = spawn_link(fun() -> push_loop(Parent, Push, 0) end),
    Receiver = spawn_link(fun() -> pull_loop(Parent, Pull, 0) end),
    timer:sleep(DurationMs),
    Sender ! stop,
    Receiver ! stop,
    Sent = receive {push_done, SentCount} -> SentCount after 5000 -> timeout end,
    Recv = receive {pull_done, RecvCount} -> RecvCount after 5000 -> timeout end,
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx),
    true = is_integer(Sent),
    true = is_integer(Recv),
    true = Recv > 0,
    io:format("[push_pull] sent=~B recv=~B~n", [Sent, Recv]).

push_loop(Parent, Push, Count) ->
    receive
        stop ->
            Parent ! {push_done, Count}
    after 0 ->
        case omq:send(Push, <<"soak">>) of
            ok -> push_loop(Parent, Push, Count + 1);
            {error, timeout, _} -> push_loop(Parent, Push, Count);
            {error, would_block, _} -> push_loop(Parent, Push, Count)
        end
    end.

pull_loop(Parent, Pull, Count) ->
    receive
        stop ->
            Parent ! {pull_done, Count}
    after 0 ->
        case omq:recv(Pull, 100) of
            {ok, <<"soak">>} -> pull_loop(Parent, Pull, Count + 1);
            {error, timeout, _} -> pull_loop(Parent, Pull, Count);
            {error, would_block, _} -> pull_loop(Parent, Pull, Count)
        end
    end.

req_rep(DurationMs) ->
    {ok, Ctx} = omq:context(),
    {ok, Rep} = omq:socket(Ctx, rep),
    {ok, Req} = omq:socket(Ctx, req),
    ok = omq:setsockopt(Rep, rcvtimeo, 100),
    ok = omq:setsockopt(Req, rcvtimeo, 1000),
    {ok, Endpoint} = omq:bind(Rep, <<"tcp://127.0.0.1:0">>),
    ok = omq:connect(Req, Endpoint),
    Parent = self(),
    Server = spawn_link(fun() -> rep_loop(Parent, Rep, 0) end),
    Cycles = req_loop(Req, deadline(DurationMs), 0),
    Server ! stop,
    Replies = receive {rep_done, Count} -> Count after 5000 -> timeout end,
    ok = omq:close(Req),
    ok = omq:close(Rep),
    ok = omq:term(Ctx),
    true = is_integer(Replies),
    true = Cycles > 0,
    io:format("[req_rep] cycles=~B replies=~B~n", [Cycles, Replies]).

rep_loop(Parent, Rep, Count) ->
    receive
        stop ->
            Parent ! {rep_done, Count}
    after 0 ->
        case omq:recv(Rep, 100) of
            {ok, Msg} ->
                ok = omq:send(Rep, Msg),
                rep_loop(Parent, Rep, Count + 1);
            {error, timeout, _} ->
                rep_loop(Parent, Rep, Count)
        end
    end.

req_loop(Req, Deadline, Count) ->
    case monotonic_ms() >= Deadline of
        true ->
            Count;
        false ->
            Msg = integer_to_binary(Count),
            ok = omq:send(Req, Msg),
            {ok, Msg} = omq:recv(Req, 1000),
            req_loop(Req, Deadline, Count + 1)
    end.

peer_churn(DurationMs) ->
    {ok, Ctx} = omq:context(),
    {ok, Push} = omq:socket(Ctx, push),
    ok = omq:setsockopt(Push, sndtimeo, 1),
    ok = omq:setsockopt(Push, sndhwm, 1024),
    {ok, Endpoint} = omq:bind(Push, <<"tcp://127.0.0.1:0">>),
    Peers = make_peers(Ctx, Endpoint, 8),
    {FinalPeers, {Sent, Partitions, Heals, Replaced}} =
        churn_loop(Ctx, Push, Endpoint, Peers, deadline(DurationMs), {0, 0, 0, 0}),
    lists:foreach(fun({Sock, _Connected}) -> ok = omq:close(Sock) end, FinalPeers),
    ok = omq:close(Push),
    ok = omq:term(Ctx),
    true = Sent > 0,
    io:format(
        "[peer_churn] sent=~B partitions=~B heals=~B replaced=~B~n",
        [Sent, Partitions, Heals, Replaced]
    ).

make_peers(_Ctx, _Endpoint, 0) ->
    [];
make_peers(Ctx, Endpoint, Count) ->
    {ok, Pull} = omq:socket(Ctx, pull),
    ok = omq:setsockopt(Pull, rcvtimeo, 0),
    ok = omq:connect(Pull, Endpoint),
    [{Pull, true} | make_peers(Ctx, Endpoint, Count - 1)].

churn_loop(Ctx, Push, Endpoint, Peers, Deadline, Stats) ->
    case monotonic_ms() >= Deadline of
        true ->
            {Peers, Stats};
        false ->
            {Peers1, Stats1} = churn_once(Ctx, Endpoint, Peers, Stats),
            Stats2 = send_batch(Push, 100, Stats1),
            drain_peers(Peers1),
            timer:sleep(100),
            churn_loop(Ctx, Push, Endpoint, Peers1, Deadline, Stats2)
    end.

churn_once(Ctx, Endpoint, Peers, {Sent, Partitions, Heals, Replaced}) ->
    Roll = rand:uniform(100),
    case Roll of
        N when N =< 15 ->
            partition_or_heal(Endpoint, Peers, {Sent, Partitions, Heals, Replaced});
        N when N =< 20 ->
            Index = rand:uniform(length(Peers)),
            {Old, _} = lists:nth(Index, Peers),
            ok = omq:close(Old),
            {ok, New} = omq:socket(Ctx, pull),
            ok = omq:setsockopt(New, rcvtimeo, 0),
            ok = omq:connect(New, Endpoint),
            {replace_nth(Index, {New, true}, Peers), {Sent, Partitions, Heals, Replaced + 1}};
        _ ->
            {Peers, {Sent, Partitions, Heals, Replaced}}
    end.

partition_or_heal(Endpoint, Peers, {Sent, Partitions, Heals, Replaced}) ->
    Disconnected = indexed(fun({_Sock, Connected}) -> not Connected end, Peers),
    Connected = indexed(fun({_Sock, Connected}) -> Connected end, Peers),
    case {Disconnected, Connected} of
        {[Index | _], _} ->
            {Sock, false} = lists:nth(Index, Peers),
            ok = omq:connect(Sock, Endpoint),
            {replace_nth(Index, {Sock, true}, Peers), {Sent, Partitions, Heals + 1, Replaced}};
        {[], [Index | _]} ->
            {Sock, true} = lists:nth(Index, Peers),
            ok = omq:disconnect(Sock, Endpoint),
            {replace_nth(Index, {Sock, false}, Peers), {Sent, Partitions + 1, Heals, Replaced}}
    end.

indexed(Pred, List) ->
    [Index || {Item, Index} <- lists:zip(List, lists:seq(1, length(List))), Pred(Item)].

replace_nth(1, Value, [_ | Rest]) ->
    [Value | Rest];
replace_nth(Index, Value, [Head | Rest]) ->
    [Head | replace_nth(Index - 1, Value, Rest)].

send_batch(_Push, 0, Stats) ->
    Stats;
send_batch(Push, Remaining, {Sent, Partitions, Heals, Replaced}) ->
    case omq:send(Push, <<"soak">>) of
        ok -> send_batch(Push, Remaining - 1, {Sent + 1, Partitions, Heals, Replaced});
        {error, timeout, _} -> {Sent, Partitions, Heals, Replaced};
        {error, would_block, _} -> {Sent, Partitions, Heals, Replaced}
    end.

drain_peers(Peers) ->
    lists:foreach(fun
        ({_Sock, false}) -> ok;
        ({Sock, true}) -> drain_peer(Sock)
    end, Peers).

drain_peer(Sock) ->
    case omq:try_recv(Sock) of
        {ok, _} -> drain_peer(Sock);
        {error, would_block, _} -> ok
    end.

deadline(DurationMs) ->
    monotonic_ms() + DurationMs.

monotonic_ms() ->
    erlang:monotonic_time(millisecond).
