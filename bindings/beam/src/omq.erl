-module(omq).

-export([
    context/0,
    context/1,
    term/1,
    socket/2,
    bind/2,
    connect/2,
    unbind/2,
    disconnect/2,
    send/2,
    send/3,
    send_multipart/2,
    send_multipart/3,
    try_send/2,
    try_send/3,
    recv/1,
    recv/2,
    try_recv/1,
    recv_multipart/1,
    recv_multipart/2,
    try_recv_multipart/1,
    poll/2,
    select/4,
    has/1,
    subscribe/2,
    unsubscribe/2,
    join/2,
    leave/2,
    send_group/3,
    close/1,
    close/2,
    wait_connected/3,
    wait_subscribed/3,
    setsockopt/3,
    getsockopt/2,
    socket_type/1
]).

-export([
    pair/0, pub/0, sub/0, req/0, rep/0, dealer/0, router/0, pull/0, push/0,
    xpub/0, xsub/0, stream/0, server/0, client/0, radio/0, dish/0,
    gather/0, scatter/0, peer/0, channel/0
]).

-export([
    pollin/0, pollout/0, pollerr/0,
    affinity/0, identity/0, routing_id/0, subscribe_opt/0, unsubscribe_opt/0, rcvmore/0,
    fd/0, events/0, type/0, backlog/0,
    linger/0, reconnect_ivl/0, reconnect_ivl_max/0, maxmsgsize/0,
    sndhwm/0, rcvhwm/0, rcvtimeo/0, sndtimeo/0, router_mandatory/0, tcp_keepalive/0,
    tcp_keepalive_cnt/0, tcp_keepalive_idle/0, tcp_keepalive_intvl/0,
    sndbuf/0, rcvbuf/0, conflate/0, handshake_ivl/0, heartbeat_ivl/0,
    heartbeat_ttl/0, heartbeat_timeout/0, reconnect_stop/0, immediate/0,
    ipv6/0, ipv4only/0, rate/0, connect_timeout/0, xpub_verbose/0,
    probe_router/0, req_correlate/0, req_relaxed/0, router_handover/0,
    tcp_accept_filter/0, tcp_maxrt/0, multicast_hops/0, recovery_ivl/0,
    zap_domain/0, mechanism/0, plain_server/0, plain_username/0,
    plain_password/0, curve_server/0, curve_publickey/0, curve_secretkey/0,
    curve_serverkey/0, last_endpoint/0, omq_on_mute/0, omq_compression_level/0,
    omq_compression_dict/0, omq_compression_auto_train/0, omq_workload_profile/0
]).

context() ->
    context(1).

context(IoThreads) ->
    omq_nif:context_new(IoThreads).

term(Context) ->
    omq_nif:context_term(Context).

socket(Context, Type) when is_atom(Type) ->
    socket(Context, socket_type_code(Type));
socket(Context, Type) when is_integer(Type) ->
    omq_nif:socket_new(Context, Type).

bind(Socket, Endpoint) ->
    omq_nif:bind(Socket, iolist_to_binary(Endpoint)).

connect(Socket, Endpoint) ->
    omq_nif:connect(Socket, iolist_to_binary(Endpoint)).

unbind(Socket, Endpoint) ->
    omq_nif:unbind(Socket, iolist_to_binary(Endpoint)).

disconnect(Socket, Endpoint) ->
    omq_nif:disconnect(Socket, iolist_to_binary(Endpoint)).

send(Socket, Data) ->
    send(Socket, Data, []).

send(Socket, Data, Opts) ->
    RoutingId = proplists:get_value(routing_id, Opts, 0),
    omq_nif:send(Socket, [iolist_to_binary(Data)], RoutingId).

send_multipart(Socket, Parts) ->
    send_multipart(Socket, Parts, []).

send_multipart(Socket, Parts, Opts) ->
    RoutingId = proplists:get_value(routing_id, Opts, 0),
    omq_nif:send(Socket, [iolist_to_binary(Part) || Part <- Parts], RoutingId).

try_send(Socket, Data) ->
    try_send(Socket, Data, []).

try_send(Socket, Data, Opts) ->
    RoutingId = proplists:get_value(routing_id, Opts, 0),
    omq_nif:try_send(Socket, [iolist_to_binary(Data)], RoutingId).

recv(Socket) ->
    case omq_nif:recv(Socket, -2) of
        {ok, [Part], 0} -> {ok, Part};
        {ok, [Part], RoutingId} -> {ok, #{data => Part, routing_id => RoutingId}};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => routing_id(RoutingId)}};
        Error -> Error
    end.

recv(Socket, Timeout) ->
    case omq_nif:recv(Socket, timeout_ms(Timeout)) of
        {ok, [Part], 0} -> {ok, Part};
        {ok, [Part], RoutingId} -> {ok, #{data => Part, routing_id => RoutingId}};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => routing_id(RoutingId)}};
        Error -> Error
    end.

try_recv(Socket) ->
    case omq_nif:try_recv(Socket) of
        {ok, [Part], 0} -> {ok, Part};
        {ok, [Part], RoutingId} -> {ok, #{data => Part, routing_id => RoutingId}};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => routing_id(RoutingId)}};
        Error -> Error
    end.

recv_multipart(Socket) ->
    case omq_nif:recv(Socket, -2) of
        {ok, Parts, 0} -> {ok, Parts};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => RoutingId}};
        Error -> Error
    end.

try_recv_multipart(Socket) ->
    case omq_nif:try_recv(Socket) of
        {ok, Parts, 0} -> {ok, Parts};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => RoutingId}};
        Error -> Error
    end.

recv_multipart(Socket, Timeout) ->
    case omq_nif:recv(Socket, timeout_ms(Timeout)) of
        {ok, Parts, 0} -> {ok, Parts};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => RoutingId}};
        Error -> Error
    end.

poll(Entries, Timeout) ->
    Normalized = [normalize_poll_entry(Entry) || Entry <- Entries],
    ReadyOut = [{Socket, pollout()} || {Socket, Flags} <- Normalized, (Flags band pollout()) =/= 0],
    InEntries = [{Socket, Flags} || {Socket, Flags} <- Normalized, (Flags band pollin()) =/= 0],
    Wait = case ReadyOut of
        [] -> timeout_ms(Timeout);
        _ -> 0
    end,
    case omq_nif:wait_any([Socket || {Socket, _Flags} <- InEntries], Wait) of
        {ok, Indexes} ->
            ReadyIn = [poll_ready_entry(InEntries, Index) || Index <- Indexes],
            {ok, merge_poll_ready(ReadyOut ++ ReadyIn)};
        Error ->
            Error
    end.

select(RList, WList, _XList, Timeout) ->
    Entries = [{Socket, pollin()} || Socket <- RList] ++ [{Socket, pollout()} || Socket <- WList],
    case poll(Entries, Timeout) of
        {ok, Ready} ->
            RReady = [Socket || {Socket, Flags} <- Ready, (Flags band pollin()) =/= 0],
            WReady = [Socket || {Socket, Flags} <- Ready, (Flags band pollout()) =/= 0],
            {ok, RReady, WReady, []};
        Error ->
            Error
    end.

has(Capability) when is_atom(Capability) ->
    has(atom_to_binary(Capability, utf8));
has(Capability) when is_list(Capability) ->
    has(list_to_binary(Capability));
has(Capability) when is_binary(Capability) ->
    case string:lowercase(binary_to_list(Capability)) of
        "ipc" -> true;
        "inproc" -> true;
        "tcp" -> true;
        "pgm" -> false;
        "gssapi" -> false;
        "curve" -> true;
        "plain" -> true;
        "lz4" -> true;
        "zstd" -> true;
        _ -> false
    end.

subscribe(Socket, Prefix) ->
    omq_nif:subscribe(Socket, iolist_to_binary(Prefix)).

unsubscribe(Socket, Prefix) ->
    omq_nif:unsubscribe(Socket, iolist_to_binary(Prefix)).

join(Socket, Group) ->
    omq_nif:join(Socket, iolist_to_binary(Group)).

leave(Socket, Group) ->
    omq_nif:leave(Socket, iolist_to_binary(Group)).

send_group(Socket, Group, Body) ->
    omq_nif:send_group(Socket, iolist_to_binary(Group), iolist_to_binary(Body)).

close(Socket) ->
    close(Socket, 0).

close(Socket, Linger) ->
    omq_nif:close(Socket, timeout_ms(Linger)).

wait_connected(Socket, MinPeers, Timeout) ->
    omq_nif:wait_connected(Socket, MinPeers, timeout_ms(Timeout)).

wait_subscribed(Socket, MinSubscriptions, Timeout) ->
    omq_nif:wait_subscribed(Socket, MinSubscriptions, timeout_ms(Timeout)).

setsockopt(Socket, Option, Value) when is_binary(Value); is_list(Value) ->
    case option_code(Option) of
        6 -> subscribe(Socket, Value);
        7 -> unsubscribe(Socket, Value);
        Code -> omq_nif:setsockopt(Socket, Code, 0, iolist_to_binary(Value))
    end;
setsockopt(Socket, Option, Value) when is_boolean(Value) ->
    omq_nif:setsockopt(Socket, option_code(Option), bool_int(Value), <<>>);
setsockopt(Socket, Option, Value) when is_integer(Value) ->
    omq_nif:setsockopt(Socket, option_code(Option), Value, <<>>).

getsockopt(Socket, Option) ->
    omq_nif:getsockopt(Socket, option_code(Option)).

socket_type(Socket) ->
    case omq_nif:socket_type(Socket) of
        {ok, Code} -> {ok, socket_type_atom(Code)};
        Error -> Error
    end.

timeout_ms(infinity) -> -1;
timeout_ms(Value) when is_integer(Value), Value >= 0 -> Value.

routing_id(0) -> undefined;
routing_id(Value) -> Value.

bool_int(true) -> 1;
bool_int(false) -> 0.

normalize_poll_entry({Socket, Flags}) -> {Socket, Flags};
normalize_poll_entry(Socket) -> {Socket, pollin()}.

poll_ready_entry(InEntries, Index) ->
    {Socket, _Flags} = lists:nth(Index + 1, InEntries),
    {Socket, pollin()}.

merge_poll_ready(Entries) ->
    maps:to_list(lists:foldl(fun({Socket, Flags}, Acc) ->
        maps:update_with(Socket, fun(Existing) -> Existing bor Flags end, Flags, Acc)
    end, #{}, Entries)).

pollin() -> 1.
pollout() -> 2.
pollerr() -> 4.

pair() -> 0.
pub() -> 1.
sub() -> 2.
req() -> 3.
rep() -> 4.
dealer() -> 5.
router() -> 6.
pull() -> 7.
push() -> 8.
xpub() -> 9.
xsub() -> 10.
stream() -> 11.
server() -> 12.
client() -> 13.
radio() -> 14.
dish() -> 15.
gather() -> 16.
scatter() -> 17.
peer() -> 19.
channel() -> 20.

affinity() -> 4.
identity() -> 5.
routing_id() -> 5.
subscribe_opt() -> 6.
unsubscribe_opt() -> 7.
rcvmore() -> 13.
fd() -> 14.
events() -> 15.
type() -> 16.
linger() -> 17.
reconnect_ivl() -> 18.
backlog() -> 19.
reconnect_ivl_max() -> 21.
maxmsgsize() -> 22.
sndhwm() -> 23.
rcvhwm() -> 24.
rcvtimeo() -> 27.
sndtimeo() -> 28.
router_mandatory() -> 33.
tcp_keepalive() -> 34.
tcp_keepalive_cnt() -> 35.
tcp_keepalive_idle() -> 36.
tcp_keepalive_intvl() -> 37.
immediate() -> 39.
ipv6() -> 42.
mechanism() -> 43.
plain_server() -> 44.
plain_username() -> 45.
plain_password() -> 46.
curve_server() -> 47.
curve_publickey() -> 48.
curve_secretkey() -> 49.
curve_serverkey() -> 50.
conflate() -> 54.
router_handover() -> 56.
handshake_ivl() -> 66.
heartbeat_ivl() -> 75.
heartbeat_ttl() -> 76.
heartbeat_timeout() -> 77.
connect_timeout() -> 79.
tcp_maxrt() -> 80.
reconnect_stop() -> 109.
rate() -> 8.
sndbuf() -> 11.
rcvbuf() -> 12.
last_endpoint() -> 32.
ipv4only() -> 31.
tcp_accept_filter() -> 38.
xpub_verbose() -> 40.
probe_router() -> 51.
req_correlate() -> 52.
req_relaxed() -> 53.
zap_domain() -> 55.
multicast_hops() -> 25.
recovery_ivl() -> 9.
omq_on_mute() -> 1004.
omq_compression_level() -> 1005.
omq_compression_dict() -> 1006.
omq_compression_auto_train() -> 1007.
omq_workload_profile() -> 1100.

socket_type_code(pair) -> pair();
socket_type_code(pub) -> pub();
socket_type_code(sub) -> sub();
socket_type_code(req) -> req();
socket_type_code(rep) -> rep();
socket_type_code(dealer) -> dealer();
socket_type_code(router) -> router();
socket_type_code(pull) -> pull();
socket_type_code(push) -> push();
socket_type_code(xpub) -> xpub();
socket_type_code(xsub) -> xsub();
socket_type_code(stream) -> stream();
socket_type_code(server) -> server();
socket_type_code(client) -> client();
socket_type_code(radio) -> radio();
socket_type_code(dish) -> dish();
socket_type_code(gather) -> gather();
socket_type_code(scatter) -> scatter();
socket_type_code(peer) -> peer();
socket_type_code(channel) -> channel().

socket_type_atom(0) -> pair;
socket_type_atom(1) -> pub;
socket_type_atom(2) -> sub;
socket_type_atom(3) -> req;
socket_type_atom(4) -> rep;
socket_type_atom(5) -> dealer;
socket_type_atom(6) -> router;
socket_type_atom(7) -> pull;
socket_type_atom(8) -> push;
socket_type_atom(9) -> xpub;
socket_type_atom(10) -> xsub;
socket_type_atom(11) -> stream;
socket_type_atom(12) -> server;
socket_type_atom(13) -> client;
socket_type_atom(14) -> radio;
socket_type_atom(15) -> dish;
socket_type_atom(16) -> gather;
socket_type_atom(17) -> scatter;
socket_type_atom(19) -> peer;
socket_type_atom(20) -> channel.

option_code(Option) when is_integer(Option) -> Option;
option_code(affinity) -> affinity();
option_code(identity) -> identity();
option_code(routing_id) -> routing_id();
option_code(subscribe) -> subscribe_opt();
option_code(subscribe_opt) -> subscribe_opt();
option_code(unsubscribe) -> unsubscribe_opt();
option_code(unsubscribe_opt) -> unsubscribe_opt();
option_code(rcvmore) -> rcvmore();
option_code(fd) -> fd();
option_code(events) -> events();
option_code(type) -> type();
option_code(backlog) -> backlog();
option_code(linger) -> linger();
option_code(reconnect_ivl) -> reconnect_ivl();
option_code(reconnect_ivl_max) -> reconnect_ivl_max();
option_code(maxmsgsize) -> maxmsgsize();
option_code(sndhwm) -> sndhwm();
option_code(rcvhwm) -> rcvhwm();
option_code(rcvtimeo) -> rcvtimeo();
option_code(sndtimeo) -> sndtimeo();
option_code(router_mandatory) -> router_mandatory();
option_code(tcp_keepalive) -> tcp_keepalive();
option_code(tcp_keepalive_cnt) -> tcp_keepalive_cnt();
option_code(tcp_keepalive_idle) -> tcp_keepalive_idle();
option_code(tcp_keepalive_intvl) -> tcp_keepalive_intvl();
option_code(sndbuf) -> sndbuf();
option_code(rcvbuf) -> rcvbuf();
option_code(conflate) -> conflate();
option_code(handshake_ivl) -> handshake_ivl();
option_code(heartbeat_ivl) -> heartbeat_ivl();
option_code(heartbeat_ttl) -> heartbeat_ttl();
option_code(heartbeat_timeout) -> heartbeat_timeout();
option_code(reconnect_stop) -> reconnect_stop();
option_code(immediate) -> immediate();
option_code(ipv6) -> ipv6();
option_code(ipv4only) -> ipv4only();
option_code(rate) -> rate();
option_code(connect_timeout) -> connect_timeout();
option_code(xpub_verbose) -> xpub_verbose();
option_code(probe_router) -> probe_router();
option_code(req_correlate) -> req_correlate();
option_code(req_relaxed) -> req_relaxed();
option_code(router_handover) -> router_handover();
option_code(tcp_accept_filter) -> tcp_accept_filter();
option_code(tcp_maxrt) -> tcp_maxrt();
option_code(multicast_hops) -> multicast_hops();
option_code(recovery_ivl) -> recovery_ivl();
option_code(zap_domain) -> zap_domain();
option_code(mechanism) -> mechanism();
option_code(plain_server) -> plain_server();
option_code(plain_username) -> plain_username();
option_code(plain_password) -> plain_password();
option_code(curve_server) -> curve_server();
option_code(curve_publickey) -> curve_publickey();
option_code(curve_secretkey) -> curve_secretkey();
option_code(curve_serverkey) -> curve_serverkey();
option_code(last_endpoint) -> last_endpoint();
option_code(omq_on_mute) -> omq_on_mute();
option_code(omq_compression_level) -> omq_compression_level();
option_code(omq_compression_dict) -> omq_compression_dict();
option_code(omq_compression_auto_train) -> omq_compression_auto_train();
option_code(omq_workload_profile) -> omq_workload_profile().
