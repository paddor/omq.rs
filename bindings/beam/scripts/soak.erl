#!/usr/bin/env escript
%%! -pa _build/default/lib/omq/ebin

-include_lib("kernel/include/file.hrl").

main(Args) ->
    code:add_patha("_build/default/lib/omq/ebin"),
    ensure_priv(),
    DurationMs = duration_ms(Args),
    io:format("OMQ.beam soak: ~B ms~n", [DurationMs]),
    run_resource_scenario(push_pull, DurationMs, fun push_pull/1),
    run_resource_scenario(req_rep, DurationMs, fun req_rep/1),
    run_resource_scenario(peer_churn, DurationMs, fun peer_churn/1),
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

run_resource_scenario(Name, DurationMs, Fun) ->
    Monitor = start_resource_monitor(Name),
    try Fun(DurationMs)
    after stop_resource_monitor(Name, Monitor)
    end.

start_resource_monitor(Name) ->
    Started = monotonic_ms(),
    Baseline = sample_resources(),
    log_resources(Name, 0, Baseline),
    Pid = spawn_link(fun() ->
        resource_monitor(Name, Started, Baseline, [{0, Baseline}])
    end),
    {Pid, Started, Baseline}.

stop_resource_monitor(Name, {Pid, Started, Baseline}) ->
    Pid ! {stop, self()},
    Samples = receive
        {resource_samples, Pid, Value} -> Value
    after 5000 ->
        erlang:error({resource_monitor_timeout, Name})
    end,
    settle_resources(),
    Final = sample_resources(),
    log_resources(Name, monotonic_ms() - Started, Final),
    trace_fds(Name),
    assert_final_resources(Name, Baseline, Final, Samples).

resource_monitor(Name, Started, Baseline, Samples) ->
    receive
        {stop, Parent} ->
            Parent ! {resource_samples, self(), Samples}
    after report_interval_ms() ->
        Elapsed = monotonic_ms() - Started,
        Current = sample_resources(),
        log_resources(Name, Elapsed, Current),
        Samples1 = Samples ++ [{Elapsed, Current}],
        assert_live_resources(Name, Baseline, Current, Samples1),
        resource_monitor(Name, Started, Baseline, Samples1)
    end.

report_interval_ms() ->
    env_int("OMQ_BEAM_SOAK_REPORT_INTERVAL_SECS", 10) * 1000.

sample_resources() ->
    Status = proc_status(),
    #{
        rss_kib => maps:get(rss_kib, Status, 0),
        vmdata_kib => maps:get(vmdata_kib, Status, 0),
        threads => maps:get(threads, Status, 0),
        fds => fd_count()
    }.

proc_status() ->
    case file:read_file("/proc/self/status") of
        {ok, Data} -> parse_proc_status(Data);
        _Error -> #{rss_kib => 0, vmdata_kib => 0, threads => 0}
    end.

parse_proc_status(Data) ->
    Lines = binary:split(Data, <<"\n">>, [global]),
    lists:foldl(fun parse_proc_line/2, #{rss_kib => 0, vmdata_kib => 0, threads => 0}, Lines).

parse_proc_line(<<"VmRSS:", Rest/binary>>, Acc) ->
    Acc#{rss_kib => proc_line_int(Rest)};
parse_proc_line(<<"VmData:", Rest/binary>>, Acc) ->
    Acc#{vmdata_kib => proc_line_int(Rest)};
parse_proc_line(<<"Threads:", Rest/binary>>, Acc) ->
    Acc#{threads => proc_line_int(Rest)};
parse_proc_line(_Line, Acc) ->
    Acc.

proc_line_int(Line) ->
    case string:lexemes(binary_to_list(Line), " \t") of
        [Value | _] -> list_to_integer(Value);
        [] -> 0
    end.

fd_count() ->
    case file:list_dir("/proc/self/fd") of
        {ok, Entries} -> length(Entries);
        _Error -> 0
    end.

settle_resources() ->
    erlang:garbage_collect(),
    timer:sleep(env_int("OMQ_BEAM_SOAK_SETTLE_MS", 100)),
    erlang:garbage_collect().

trace_fds(Name) ->
    case os:getenv("OMQ_BEAM_SOAK_TRACE_FD") of
        "1" ->
            case file:list_dir("/proc/self/fd") of
                {ok, Entries} ->
                    lists:foreach(fun(Entry) -> trace_fd(Name, Entry) end, lists:sort(Entries));
                _Error ->
                    ok
            end;
        _Other ->
            ok
    end.

trace_fd(Name, Entry) ->
    Path = filename:join("/proc/self/fd", Entry),
    Target = case file:read_link(Path) of
        {ok, Link} -> Link;
        _Error -> "unknown"
    end,
    io:format("[beam-soak-fd] ~s ~s -> ~s~n", [atom_to_list(Name), Entry, Target]).

log_resources(Name, ElapsedMs, Sample) ->
    io:format(
        "[beam-soak] ~s ~Bs rss=~.1fMB vmdata=~.1fMB fds=~B threads=~B~n",
        [
            atom_to_list(Name),
            ElapsedMs div 1000,
            resource_mb(rss_kib, Sample),
            resource_mb(vmdata_kib, Sample),
            maps:get(fds, Sample),
            maps:get(threads, Sample)
        ]
    ).

resource_mb(Key, Sample) ->
    maps:get(Key, Sample) / 1024.

assert_live_resources(Name, Baseline, Current, Samples) ->
    assert_count_growth(
        Name,
        "FD",
        maps:get(fds, Baseline),
        maps:get(fds, Current),
        env_int("OMQ_BEAM_SOAK_MAX_FD_GROWTH", 128)
    ),
    assert_count_growth(
        Name,
        "thread",
        maps:get(threads, Baseline),
        maps:get(threads, Current),
        env_int("OMQ_BEAM_SOAK_MAX_THREAD_GROWTH", 8)
    ),
    assert_live_slope(
        Name,
        "RSS",
        Samples,
        fun(Sample) -> maps:get(rss_kib, Sample) end,
        env_float("OMQ_BEAM_SOAK_RSS_SLOPE_LIMIT_KIB_S", 1024.0),
        env_int("OMQ_BEAM_SOAK_RSS_SLOPE_MIN_GROWTH_MB", 128) * 1024
    ),
    assert_live_slope(
        Name,
        "FD",
        Samples,
        fun(Sample) -> maps:get(fds, Sample) end,
        env_float("OMQ_BEAM_SOAK_FD_SLOPE_LIMIT_PER_SEC", 0.05),
        env_int("OMQ_BEAM_SOAK_FD_SLOPE_MIN_GROWTH", 32)
    ).

assert_count_growth(_Name, _Metric, 0, _Current, _Limit) ->
    ok;
assert_count_growth(Name, Metric, Baseline, Current, Limit) when Current > Baseline + Limit ->
    erlang:error({resource_growth, Name, Metric, Baseline, Current, Limit});
assert_count_growth(_Name, _Metric, _Baseline, _Current, _Limit) ->
    ok.

assert_live_slope(Name, Metric, Samples, ValueFun, Limit, MinGrowth) ->
    Warmup = env_int("OMQ_BEAM_SOAK_RESOURCE_WARMUP_SECS", 600) * 1000,
    Window = env_int("OMQ_BEAM_SOAK_RESOURCE_WINDOW_SECS", 300) * 1000,
    MinSamples = env_int("OMQ_BEAM_SOAK_RESOURCE_MIN_SAMPLES", 12),
    case live_window(Samples, Warmup, Window, MinSamples) of
        {ok, [{StartMs, StartSample} | _] = WindowSamples} ->
            {EndMs, EndSample} = lists:last(WindowSamples),
            StartValue = ValueFun(StartSample),
            EndValue = ValueFun(EndSample),
            Growth = saturating_sub(EndValue, StartValue),
            Seconds = max((EndMs - StartMs) / 1000, 1.0),
            Slope = Growth / Seconds,
            case Growth >= MinGrowth andalso Slope > Limit of
                true -> erlang:error({resource_slope, Name, Metric, Growth, Slope, Limit});
                false -> ok
            end;
        skip ->
            ok
    end.

live_window(Samples, Warmup, Window, MinSamples) ->
    case length(Samples) >= MinSamples of
        false ->
            skip;
        true ->
            {Elapsed, _} = lists:last(Samples),
            case Elapsed >= Warmup + Window of
                false ->
                    skip;
                true ->
                    WindowStart = Elapsed - Window,
                    WindowSamples = [{At, Sample} || {At, Sample} <- Samples, At >= WindowStart],
                    case length(WindowSamples) >= MinSamples of
                        true -> {ok, WindowSamples};
                        false -> skip
                    end
            end
    end.

assert_final_resources(Name, Baseline, Final, Samples) ->
    assert_count_growth(
        Name,
        "final FD",
        maps:get(fds, Baseline),
        maps:get(fds, Final),
        env_int("OMQ_BEAM_SOAK_MAX_FINAL_FD_GROWTH", 16)
    ),
    assert_count_growth(
        Name,
        "final thread",
        maps:get(threads, Baseline),
        maps:get(threads, Final),
        env_int("OMQ_BEAM_SOAK_MAX_FINAL_THREAD_GROWTH", 4)
    ),
    assert_rss_residual(Name, Final, Samples).

assert_rss_residual(Name, Final, Samples) ->
    Warmup = env_int("OMQ_BEAM_SOAK_RESOURCE_WARMUP_SECS", 600) * 1000,
    MinSamples = env_int("OMQ_BEAM_SOAK_RESOURCE_MIN_SAMPLES", 12),
    Warm = [Sample || {At, Sample} <- Samples, At >= Warmup],
    case length(Warm) >= MinSamples of
        false ->
            ok;
        true ->
            BaseCount = max(length(Warm) div 10, 1),
            TailCount = max(length(Warm) div 5, 1),
            Base = average([maps:get(rss_kib, Sample) || Sample <- lists:sublist(Warm, BaseCount)]),
            Tail = lists:max([maps:get(rss_kib, Sample) || Sample <- last_n(Warm, TailCount)]),
            FinalRss = maps:get(rss_kib, Final),
            TailGrowth = saturating_sub(Tail, Base),
            FinalGrowth = saturating_sub(FinalRss, Base),
            MinGrowth = env_int("OMQ_BEAM_SOAK_RSS_TAIL_GROWTH_MIN_MB", 128) * 1024,
            Limit = env_float("OMQ_BEAM_SOAK_RSS_TAIL_GROWTH_PERCENT", 25.0),
            io:format(
                "[beam-soak] ~s RSS baseline=~.1fMB tail-max=~.1fMB final=~.1fMB growth=~.1f%~n",
                [
                    atom_to_list(Name),
                    Base / 1024,
                    Tail / 1024,
                    FinalRss / 1024,
                    percent_growth(FinalGrowth, Base)
                ]
            ),
            case TailGrowth >= MinGrowth
                    andalso FinalGrowth >= MinGrowth
                    andalso percent_growth(TailGrowth, Base) > Limit
                    andalso percent_growth(FinalGrowth, Base) > Limit of
                true -> erlang:error({rss_residual, Name, Base, Tail, FinalRss, Limit});
                false -> ok
            end
    end.

last_n(List, Count) ->
    lists:nthtail(max(length(List) - Count, 0), List).

average(Values) ->
    lists:sum(Values) / max(length(Values), 1).

percent_growth(_Growth, 0) ->
    0.0;
percent_growth(Growth, Baseline) ->
    Growth * 100 / Baseline.

saturating_sub(A, B) when A > B ->
    A - B;
saturating_sub(_A, _B) ->
    0.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> list_to_integer(Value)
    end.

env_float(Name, Default) ->
    case os:getenv(Name) of
        false ->
            Default;
        Value ->
            case string:to_float(Value) of
                {Float, _Rest} when is_float(Float) -> Float;
                {error, no_float} -> list_to_integer(Value) * 1.0
            end
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
