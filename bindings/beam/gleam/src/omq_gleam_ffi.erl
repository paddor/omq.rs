-module(omq_gleam_ffi).

-export([
    context/0,
    term/1,
    destroy/1,
    context_share_key/1,
    context_from_share_key/1,
    context_closed/1,
    backend_name/0,
    version/0,
    share_key/1,
    from_share_key/1,
    socket/2,
    bind/2,
    bind_to_random_port/4,
    connect/2,
    unbind/2,
    disconnect/2,
    monitor/1,
    monitor_recv/2,
    monitor_try_recv/1,
    connections/1,
    connection_info/2,
    proxy/2,
    proxy_with_capture/3,
    proxy_steerable/4,
    device/3,
    send/2,
    send_string/2,
    send_multipart/2,
    recv/1,
    recv_string/1,
    recv_string_timeout/2,
    recv_frame/1,
    try_recv/1,
    try_recv_string/1,
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
    setsockopt_string/3,
    getsockopt_int/2,
    getsockopt_binary/2,
    getsockopt_string/2,
    set_hwm/2,
    get_hwm/1,
    socket_type/1,
    socket_id/1,
    closed/1,
    has/1,
    curve_keypair/0,
    curve_public/1
]).

context() ->
    result(omq:context()).

term(Context) ->
    result_nil(omq:term(Context)).

destroy(Context) ->
    result_nil(omq:destroy(Context)).

context_share_key(Context) ->
    result(omq:context_share_key(Context)).

context_from_share_key(ShareKey) ->
    result(omq:context_from_share_key(ShareKey)).

context_closed(Context) ->
    omq:context_closed(Context).

backend_name() ->
    result(omq:backend_name()).

version() ->
    result(omq:version()).

share_key(Context) ->
    result(omq:share_key(Context)).

from_share_key(ShareKey) ->
    result(omq:from_share_key(ShareKey)).

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

monitor(Socket) ->
    result(omq:monitor(Socket)).

monitor_recv(Monitor, Timeout) ->
    result(omq:monitor_recv(Monitor, Timeout)).

monitor_try_recv(Monitor) ->
    result(omq:monitor_try_recv(Monitor)).

connections(Socket) ->
    result(omq:connections(Socket)).

connection_info(Socket, ConnectionId) ->
    result(omq:connection_info(Socket, ConnectionId)).

proxy(Frontend, Backend) ->
    result_nil(omq:proxy(Frontend, Backend)).

proxy_with_capture(Frontend, Backend, Capture) ->
    result_nil(omq:proxy(Frontend, Backend, Capture)).

proxy_steerable(Frontend, Backend, Capture, Control) ->
    result_nil(omq:proxy_steerable(Frontend, Backend, Capture, Control)).

device(DeviceType, Frontend, Backend) ->
    result_nil(omq:device(DeviceType, Frontend, Backend)).

send(Socket, Data) ->
    result_nil(omq:send(Socket, Data)).

send_string(Socket, Text) ->
    result_nil(omq:send_string(Socket, Text)).

send_multipart(Socket, Parts) ->
    result_nil(omq:send_multipart(Socket, Parts)).

recv(Socket) ->
    result(omq:recv(Socket)).

recv_string(Socket) ->
    result(omq:recv_string(Socket)).

recv_string_timeout(Socket, Timeout) ->
    result(omq:recv_string(Socket, Timeout)).

recv_frame(Socket) ->
    result(omq:recv_frame(Socket)).

try_recv(Socket) ->
    result(omq:try_recv(Socket)).

try_recv_string(Socket) ->
    result(omq:try_recv_string(Socket)).

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

setsockopt_string(Socket, Option, Value) ->
    result_nil(omq:setsockopt_string(Socket, Option, Value)).

getsockopt_int(Socket, Option) ->
    result(omq:getsockopt(Socket, Option)).

getsockopt_binary(Socket, Option) ->
    result(omq:getsockopt(Socket, Option)).

getsockopt_string(Socket, Option) ->
    result(omq:getsockopt_string(Socket, Option)).

set_hwm(Socket, Value) ->
    result_nil(omq:set_hwm(Socket, Value)).

get_hwm(Socket) ->
    result(omq:get_hwm(Socket)).

socket_type(Socket) ->
    case omq:socket_type(Socket) of
        {ok, Type} -> {ok, atom_to_binary(Type)};
        Error -> result(Error)
    end.

socket_id(Socket) ->
    result(omq:socket_id(Socket)).

closed(Socket) ->
    omq:closed(Socket).

has(Capability) ->
    omq:has(Capability).

curve_keypair() ->
    result_pair(omq:curve_keypair()).

curve_public(Secret) ->
    result(omq:curve_public(Secret)).

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

result_pair({ok, A, B}) ->
    {ok, {A, B}};
result_pair(Other) ->
    result(Other).
