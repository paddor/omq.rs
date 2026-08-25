-module(omq_gleam_test_ffi).

-include_lib("kernel/include/file.hrl").

-export([
    endpoint/1,
    erlang_push_send/3,
    erlang_start_pull/2,
    erlang_wait_pull/1,
    ensure_omq/0
]).

endpoint(Name) ->
    Id = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    NameBin = unicode:characters_to_binary(Name),
    <<"inproc://gleam-", NameBin/binary, "-", Id/binary>>.

ensure_omq() ->
    code:add_patha("../_build/default/lib/omq/ebin"),
    Source = nif_source(),
    Target = "../_build/default/lib/omq/priv/omq_beam_native.so",
    ok = filelib:ensure_dir(Target),
    case file:copy(Source, Target) of
        {ok, _Bytes} -> nil;
        {error, same} -> nil;
        {error, Reason} -> erlang:error({nif_stage_failed, Reason})
    end.

erlang_push_send(Context, Endpoint, Body) ->
    {ok, Push} = omq:socket(Context, push),
    ok = omq:connect(Push, Endpoint),
    ok = omq:send(Push, Body),
    ok = omq:close(Push),
    nil.

erlang_start_pull(Context, Endpoint) ->
    Parent = self(),
    Pid = spawn(fun() ->
        {ok, Pull} = omq:socket(Context, pull),
        {ok, Endpoint} = omq:bind(Pull, Endpoint),
        Parent ! {self(), ready},
        Result = omq:recv(Pull, 1000),
        ok = omq:close(Pull),
        Parent ! {self(), Result}
    end),
    receive
        {Pid, ready} -> Pid
    after 1000 ->
        erlang:error(erlang_pull_not_ready)
    end.

erlang_wait_pull(Pid) ->
    receive
        {Pid, {ok, Body}} -> Body;
        {Pid, Error} -> erlang:error({erlang_pull_failed, Error})
    after 2000 ->
        erlang:error(erlang_pull_timeout)
    end.

nif_source() ->
    Candidates = [
        "../native/target/debug/libomq_beam_native.so",
        "../native/target/release/libomq_beam_native.so",
        "../priv/omq_beam_native.so"
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
