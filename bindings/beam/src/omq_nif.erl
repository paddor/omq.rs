-module(omq_nif).

-on_load(load/0).

-export([
    context_new/1,
    context_term/1,
    socket_new/2,
    socket_type/1,
    bind/2,
    connect/2,
    unbind/2,
    disconnect/2,
    send/3,
    try_send/3,
    recv/2,
    try_recv/1,
    wait_any/2,
    subscribe/2,
    unsubscribe/2,
    join/2,
    leave/2,
    send_group/3,
    close/2,
    wait_connected/3,
    wait_subscribed/3,
    setsockopt/4,
    getsockopt/2
]).

load() ->
    Priv = case code:priv_dir(omq) of
        {error, bad_name} ->
            filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]);
        Dir ->
            Dir
    end,
    erlang:load_nif(filename:join(Priv, "omq_beam_native"), 0).

context_new(_IoThreads) -> nif_error().
context_term(_Context) -> nif_error().
socket_new(_Context, _SocketType) -> nif_error().
socket_type(_Socket) -> nif_error().
bind(_Socket, _Endpoint) -> nif_error().
connect(_Socket, _Endpoint) -> nif_error().
unbind(_Socket, _Endpoint) -> nif_error().
disconnect(_Socket, _Endpoint) -> nif_error().
send(_Socket, _Parts, _RoutingId) -> nif_error().
try_send(_Socket, _Parts, _RoutingId) -> nif_error().
recv(_Socket, _TimeoutMs) -> nif_error().
try_recv(_Socket) -> nif_error().
wait_any(_Sockets, _TimeoutMs) -> nif_error().
subscribe(_Socket, _Prefix) -> nif_error().
unsubscribe(_Socket, _Prefix) -> nif_error().
join(_Socket, _Group) -> nif_error().
leave(_Socket, _Group) -> nif_error().
send_group(_Socket, _Group, _Body) -> nif_error().
close(_Socket, _LingerMs) -> nif_error().
wait_connected(_Socket, _MinPeers, _TimeoutMs) -> nif_error().
wait_subscribed(_Socket, _MinSubscriptions, _TimeoutMs) -> nif_error().
setsockopt(_Socket, _Option, _IntValue, _BinValue) -> nif_error().
getsockopt(_Socket, _Option) -> nif_error().

nif_error() ->
    erlang:nif_error(nif_not_loaded).
