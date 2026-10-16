-module(omq_gleam_test_ffi).

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
    case code:ensure_loaded(omq_nif) of
        {module, omq_nif} -> nil;
        {error, Reason} -> erlang:error({omq_nif_unavailable, Reason})
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
