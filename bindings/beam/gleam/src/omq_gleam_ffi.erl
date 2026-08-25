-module(omq_gleam_ffi).

-export([
    context/0,
    term/1,
    socket/2,
    bind/2,
    bind_to_random_port/4,
    connect/2,
    unbind/2,
    disconnect/2,
    send/2,
    send_multipart/2,
    recv/1,
    recv_frame/1,
    try_recv/1,
    recv_multipart/1,
    try_recv_multipart/1,
    subscribe/2,
    unsubscribe/2,
    join/2,
    leave/2,
    send_group/3,
    close/1,
    wait_connected/3,
    wait_subscribed/3,
    setsockopt_int/3,
    setsockopt_binary/3,
    getsockopt_int/2,
    getsockopt_binary/2,
    socket_type/1,
    has/1
]).

context() ->
    result(omq:context()).

term(Context) ->
    result_nil(omq:term(Context)).

socket(Context, SocketType) ->
    result(omq:socket(Context, SocketType)).

bind(Socket, Endpoint) ->
    result(omq:bind(Socket, Endpoint)).

bind_to_random_port(Socket, Addr, MinPort, MaxPort) ->
    result(omq:bind_to_random_port(Socket, Addr, MinPort, MaxPort)).

connect(Socket, Endpoint) ->
    result_nil(omq:connect(Socket, Endpoint)).

unbind(Socket, Endpoint) ->
    result_nil(omq:unbind(Socket, Endpoint)).

disconnect(Socket, Endpoint) ->
    result_nil(omq:disconnect(Socket, Endpoint)).

send(Socket, Data) ->
    result_nil(omq:send(Socket, Data)).

send_multipart(Socket, Parts) ->
    result_nil(omq:send_multipart(Socket, Parts)).

recv(Socket) ->
    result(omq:recv(Socket)).

recv_frame(Socket) ->
    result(omq:recv_frame(Socket)).

try_recv(Socket) ->
    result(omq:try_recv(Socket)).

recv_multipart(Socket) ->
    result(omq:recv_multipart(Socket)).

try_recv_multipart(Socket) ->
    result(omq:try_recv_multipart(Socket)).

subscribe(Socket, Prefix) ->
    result_nil(omq:subscribe(Socket, Prefix)).

unsubscribe(Socket, Prefix) ->
    result_nil(omq:unsubscribe(Socket, Prefix)).

join(Socket, Group) ->
    result_nil(omq:join(Socket, Group)).

leave(Socket, Group) ->
    result_nil(omq:leave(Socket, Group)).

send_group(Socket, Group, Body) ->
    result_nil(omq:send_group(Socket, Group, Body)).

close(Socket) ->
    result_nil(omq:close(Socket)).

wait_connected(Socket, MinPeers, Timeout) ->
    result(omq:wait_connected(Socket, MinPeers, Timeout)).

wait_subscribed(Socket, MinSubscriptions, Timeout) ->
    result(omq:wait_subscribed(Socket, MinSubscriptions, Timeout)).

setsockopt_int(Socket, Option, Value) ->
    result_nil(omq:setsockopt(Socket, Option, Value)).

setsockopt_binary(Socket, Option, Value) ->
    result_nil(omq:setsockopt(Socket, Option, Value)).

getsockopt_int(Socket, Option) ->
    result(omq:getsockopt(Socket, Option)).

getsockopt_binary(Socket, Option) ->
    result(omq:getsockopt(Socket, Option)).

socket_type(Socket) ->
    case omq:socket_type(Socket) of
        {ok, Type} -> {ok, atom_to_binary(Type)};
        Error -> result(Error)
    end.

has(Capability) ->
    omq:has(Capability).

result(ok) ->
    {ok, nil};
result({ok, Value}) ->
    {ok, Value};
result({error, Class, Reason}) ->
    {error, {atom_to_binary(Class), unicode:characters_to_binary(Reason)}}.

result_nil(ok) ->
    {ok, nil};
result_nil(Other) ->
    result(Other).
