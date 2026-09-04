%% @doc Erlang API for OMQ.
%%
%% OMQ sockets are ZeroMQ-compatible message queues backed by the Rust
%% omq-tokio runtime. Create a context with context/0 or reuse the
%% process-wide singleton with context_instance/0, then create sockets with
%% socket/2.
%%
%% Endpoints are binaries or iodata URI strings such as
%% tcp://127.0.0.1:5555, ipc:///tmp/omq.sock, inproc://queue,
%% lz4+tcp://127.0.0.1:5555, and zstd+tcp://127.0.0.1:5555.
%%
%% Most calls return ok, {ok, Value}, or {error, Class, Reason}.
%% send/3 accepts an integer flags mask or an option list. Supported option
%% entries are sndmore, noblock, dontwait, {flags, Flags}, and
%% {routing_id, Id}. recv/1,2 returns either {ok, Data} or routing
%% metadata maps for ROUTER/SERVER-style sockets.
%%
%% Socket type constants match libzmq values where libzmq defines one. Socket
%% options can be passed by atom or integer option ID.
%% @end
-module(omq).

-export([
    context/0,
    context/1,
    context_instance/0,
    context_instance/1,
    instance/0,
    instance/1,
    term/1,
    destroy/1,
    context_share_key/1,
    context_from_share_key/1,
    context_closed/1,
    backend_name/0,
    version/0,
    omq_version/0,
    omq_version_info/0,
    zmq_version/0,
    zmq_version_info/0,
    strerror/1,
    share_key/1,
    from_share_key/1,
    socket/2,
    bind/2,
    bind_to_random_port/2,
    bind_to_random_port/4,
    connect/2,
    unbind/2,
    disconnect/2,
    monitor/1,
    monitor_recv/1,
    monitor_recv/2,
    monitor_try_recv/1,
    connections/1,
    connection_info/2,
    send/2,
    send/3,
    send_string/2,
    send_string/3,
    send_string/4,
    send_json/2,
    send_json/3,
    send_term/2,
    send_term/3,
    send_multipart/2,
    send_multipart/3,
    try_send/2,
    try_send/3,
    recv/1,
    recv/2,
    recv_string/1,
    recv_string/2,
    recv_string/3,
    recv_json/1,
    recv_json/2,
    try_recv_json/1,
    recv_term/1,
    recv_term/2,
    try_recv_term/1,
    try_recv_string/1,
    try_recv_string/2,
    recv_frame/1,
    recv_frame/2,
    try_recv/1,
    recv_multipart/1,
    recv_multipart/2,
    try_recv_multipart/1,
    poll/2,
    select/4,
    proxy/2,
    proxy/3,
    proxy_steerable/4,
    device/3,
    has/1,
    curve_keypair/0,
    curve_public/1,
    plain_server/2,
    plain_server/3,
    subscribe/2,
    unsubscribe/2,
    join/2,
    leave/2,
    send_group/3,
    close/1,
    close/2,
    wait_connected/3,
    wait_subscribed/3,
    set/3,
    get/2,
    setsockopt/3,
    getsockopt/2,
    set_hwm/2,
    get_hwm/1,
    setsockopt_string/3,
    getsockopt_string/2,
    socket_type/1,
    socket_id/1,
    closed/1
]).

-export([
    pair/0, pub/0, sub/0, req/0, rep/0, dealer/0, router/0, pull/0, push/0,
    xpub/0, xsub/0, stream/0, server/0, client/0, radio/0, dish/0,
    gather/0, scatter/0, peer/0, channel/0
]).

-export([
    pollin/0, pollout/0, pollerr/0, pollpri/0, sndmore/0, noblock/0, dontwait/0,
    hwm/0, affinity/0, identity/0, routing_id/0, subscribe_opt/0, unsubscribe_opt/0, rcvmore/0,
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
    omq_compression_dict/0, omq_compression_auto_train/0, omq_workload_profile/0,
    omq_on_mute_block/0, omq_on_mute_drop_newest/0, omq_on_mute_drop_oldest/0,
    forwarder/0, queue/0, streamer/0, null/0, plain/0, curve/0
]).

%% @doc Create a context. context/0 uses one IO thread.
context() ->
    context(1).

context(IoThreads) ->
    omq_nif:context_new(IoThreads).

%% @doc Return process-wide singleton context.
context_instance() ->
    context_instance(1).

context_instance(IoThreads) ->
    global:trans({?MODULE, context_instance}, fun() ->
        Key = {?MODULE, context_instance},
        case persistent_term:get(Key, undefined) of
            undefined ->
                put_context_instance(Key, IoThreads);
            Context ->
                case context_closed(Context) of
                    true -> put_context_instance(Key, IoThreads);
                    false -> {ok, Context}
                end
        end
    end).

%% @doc Return process-wide singleton context. Alias for context_instance/0,1.
instance() ->
    context_instance().

instance(IoThreads) ->
    context_instance(IoThreads).

%% @doc Terminate a context.
term(Context) ->
    global:trans({?MODULE, context_instance}, fun() ->
        maybe_clear_context_instance(Context),
        omq_nif:context_term(Context)
    end).

%% @doc Terminate a context. Alias for term/1.
destroy(Context) ->
    term(Context).

%% @doc Return opaque native context share key.
context_share_key(Context) ->
    omq_nif:context_share_key(Context).

%% @doc Import native context by share key.
context_from_share_key(ShareKey) ->
    omq_nif:context_from_share_key(ShareKey).

%% @doc Return whether context wrapper or native core is closed.
context_closed(Context) ->
    omq_nif:context_closed(Context).

%% @doc Return native backend name.
backend_name() ->
    omq_nif:backend_name().

%% @doc Return native binding version.
version() ->
    omq_nif:version().

%% @doc Return native binding version. Alias for version/0.
omq_version() ->
    version().

%% @doc Return native binding version as {Major, Minor, Patch}.
omq_version_info() ->
    case version() of
        {ok, Version} -> {ok, version_info_tuple(Version)};
        Error -> Error
    end.

%% @doc Return libzmq compatibility version string.
zmq_version() ->
    <<"4.3.4">>.

%% @doc Return libzmq compatibility version tuple.
zmq_version_info() ->
    {4, 3, 4}.

%% @doc Return POSIX strerror text for common libzmq errno values.
strerror(Errno) when is_integer(Errno) ->
    case errno_atom(Errno) of
        undefined -> unicode:characters_to_binary("Unknown error");
        Atom -> unicode:characters_to_binary(erl_posix_msg:message(Atom))
    end.

%% @doc Return opaque native context share key.
share_key(Context) ->
    context_share_key(Context).

%% @doc Import native context by share key.
from_share_key(ShareKey) ->
    context_from_share_key(ShareKey).

%% @doc Create socket from context and socket type atom or constant.
socket(Context, Type) when is_atom(Type) ->
    socket(Context, socket_type_code(Type));
socket(Context, Type) when is_integer(Type) ->
    omq_nif:socket_new(Context, Type).

%% @doc Bind socket to endpoint and return bound endpoint.
bind(Socket, Endpoint) ->
    omq_nif:bind(Socket, iolist_to_binary(Endpoint)).

%% @doc Bind socket to random port in range.
bind_to_random_port(Socket, Addr) ->
    bind_to_random_port(Socket, Addr, 49152, 65536).

bind_to_random_port(Socket, Addr, MinPort, MaxPort)
        when is_integer(MinPort), is_integer(MaxPort), MinPort =< MaxPort ->
    bind_to_random_port_try(Socket, iolist_to_binary(Addr), MinPort, MaxPort).

%% @doc Connect socket to endpoint.
connect(Socket, Endpoint) ->
    omq_nif:connect(Socket, iolist_to_binary(Endpoint)).

%% @doc Unbind socket from endpoint.
unbind(Socket, Endpoint) ->
    omq_nif:unbind(Socket, iolist_to_binary(Endpoint)).

%% @doc Disconnect socket from endpoint.
disconnect(Socket, Endpoint) ->
    omq_nif:disconnect(Socket, iolist_to_binary(Endpoint)).

%% @doc Create monitor stream for socket lifecycle events.
monitor(Socket) ->
    omq_nif:monitor(Socket).

%% @doc Receive next monitor event, optionally with timeout.
monitor_recv(Monitor) ->
    monitor_recv(Monitor, infinity).

monitor_recv(Monitor, Timeout) ->
    omq_nif:monitor_recv(Monitor, timeout_ms(Timeout)).

%% @doc Try to receive one monitor event without blocking.
monitor_try_recv(Monitor) ->
    omq_nif:monitor_try_recv(Monitor).

%% @doc Return current connection snapshots for socket.
connections(Socket) ->
    omq_nif:connections(Socket).

%% @doc Return one connection snapshot by ID.
connection_info(Socket, ConnectionId) ->
    omq_nif:connection_info(Socket, ConnectionId).

%% @doc Send one message. Options may include flags and routing_id.
send(Socket, Data) ->
    send(Socket, Data, []).

send(Socket, Data, Opts) ->
    {RoutingId, Flags} = send_options(Opts),
    send_parts(Socket, [iolist_to_binary(Data)], RoutingId, Flags).

%% @doc Send one string message with optional encoding and options.
send_string(Socket, Text) ->
    send_string(Socket, Text, []).

send_string(Socket, Text, Encoding) when is_atom(Encoding) ->
    send_string(Socket, Text, Encoding, []);
send_string(Socket, Text, Opts) ->
    send(Socket, unicode:characters_to_binary(Text), Opts).

send_string(Socket, Text, Encoding, Opts) ->
    send(Socket, unicode:characters_to_binary(Text, utf8, Encoding), Opts).

%% @doc Send one JSON value encoded with OTP json.
send_json(Socket, Value) ->
    send_json(Socket, Value, []).

send_json(Socket, Value, Opts) ->
    send(Socket, json:encode(Value), Opts).

%% @doc Send one Erlang term using external term format.
send_term(Socket, Term) ->
    send_term(Socket, Term, []).

send_term(Socket, Term, Opts) ->
    send(Socket, term_to_binary(Term), Opts).

%% @doc Send one multipart message.
send_multipart(Socket, Parts) ->
    send_multipart(Socket, Parts, []).

send_multipart(Socket, Parts, Opts) ->
    {RoutingId, Flags} = send_options(Opts),
    send_parts(Socket, [iolist_to_binary(Part) || Part <- Parts], RoutingId, Flags).

%% @doc Try to send one message without blocking.
try_send(Socket, Data) ->
    try_send(Socket, Data, []).

try_send(Socket, Data, Opts) ->
    {RoutingId, Flags} = send_options(Opts),
    try_send_parts(Socket, [iolist_to_binary(Data)], RoutingId, Flags).

%% @doc Receive one message. Returns routing metadata when present.
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

%% @doc Receive one string message with optional timeout and encoding.
recv_string(Socket) ->
    recv_string(Socket, infinity).

recv_string(Socket, infinity) ->
    recv_string(Socket, infinity, utf8);
recv_string(Socket, Encoding) when is_atom(Encoding) ->
    recv_string(Socket, infinity, Encoding);
recv_string(Socket, Timeout) ->
    recv_string(Socket, Timeout, utf8).

recv_string(Socket, Timeout, Encoding) ->
    case recv(Socket, Timeout) of
        {ok, Data} when is_binary(Data) ->
            {ok, unicode:characters_to_binary(Data, Encoding, utf8)};
        Other ->
            Other
    end.

%% @doc Receive one JSON value decoded by OTP json.
recv_json(Socket) ->
    recv_json(Socket, infinity).

recv_json(Socket, Timeout) ->
    decode_json_result(recv(Socket, Timeout)).

%% @doc Try to receive one JSON value without blocking.
try_recv_json(Socket) ->
    decode_json_result(try_recv(Socket)).

%% @doc Receive one Erlang term encoded by send_term/2,3.
recv_term(Socket) ->
    recv_term(Socket, infinity).

recv_term(Socket, Timeout) ->
    decode_term_result(recv(Socket, Timeout)).

%% @doc Try to receive one Erlang term without blocking.
try_recv_term(Socket) ->
    decode_term_result(try_recv(Socket)).

%% @doc Receive next frame and update RCVMORE wrapper state.
recv_frame(Socket) ->
    recv_frame(Socket, infinity).

recv_frame(Socket, Timeout) ->
    case pop_recv_frame(Socket) of
        {ok, Part} ->
            {ok, Part};
        empty ->
            case omq_nif:recv(Socket, timeout_ms(Timeout)) of
                {ok, Parts, 0} -> first_recv_frame(Socket, Parts);
                {ok, Parts, _RoutingId} -> first_recv_frame(Socket, Parts);
                Error -> Error
            end
    end.

%% @doc Try to receive one message without blocking.
try_recv(Socket) ->
    case omq_nif:try_recv(Socket) of
        {ok, [Part], 0} -> {ok, Part};
        {ok, [Part], RoutingId} -> {ok, #{data => Part, routing_id => RoutingId}};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => routing_id(RoutingId)}};
        Error -> Error
    end.

%% @doc Try to receive one string without blocking.
try_recv_string(Socket) ->
    try_recv_string(Socket, utf8).

try_recv_string(Socket, Encoding) ->
    case try_recv(Socket) of
        {ok, Data} when is_binary(Data) ->
            {ok, unicode:characters_to_binary(Data, Encoding, utf8)};
        Other ->
            Other
    end.

%% @doc Receive one multipart message.
recv_multipart(Socket) ->
    case omq_nif:recv(Socket, -2) of
        {ok, Parts, 0} -> {ok, Parts};
        {ok, Parts, RoutingId} -> {ok, #{parts => Parts, routing_id => RoutingId}};
        Error -> Error
    end.

%% @doc Try to receive one multipart message without blocking.
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

%% @doc Poll socket readiness entries with timeout.
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

%% @doc Return ready read, write, and exception socket lists.
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

%% @doc Run bidirectional proxy between sockets.
proxy(Frontend, Backend) ->
    proxy(Frontend, Backend, undefined).

proxy(Frontend, Backend, Capture) ->
    proxy_loop(Frontend, Backend, Capture).

%% @doc Run steerable proxy with PAUSE, RESUME, and TERMINATE control.
proxy_steerable(Frontend, Backend, Capture, Control) ->
    proxy_steerable_loop(Frontend, Backend, Capture, Control, active).

%% @doc Run libzmq-compatible device. Device type is accepted for parity.
device(_DeviceType, Frontend, Backend) ->
    proxy(Frontend, Backend).

%% @doc Return whether native feature or transport is available.
has(Capability) when is_atom(Capability) ->
    has(atom_to_binary(Capability, utf8));
has(Capability) when is_list(Capability) ->
    has(list_to_binary(Capability));
has(Capability) when is_binary(Capability) ->
    omq_nif:has_feature(string:lowercase(Capability)).

%% @doc Generate CURVE public/secret keypair.
curve_keypair() ->
    omq_nif:curve_keypair().

%% @doc Derive CURVE public key from secret key.
curve_public(Secret) ->
    omq_nif:curve_public(iolist_to_binary(Secret)).

%% @doc Configure an exact, case-sensitive PLAIN server credential allowlist.
%%
%% Call before bind, connect, send, or receive. Each username and password
%% must contain at most 255 ASCII VCHAR bytes. An empty list rejects every
%% client. PLAIN authenticates clients but does not encrypt traffic.
plain_server(Socket, Credentials) ->
    omq_nif:plain_server_credentials(
        Socket,
        [plain_credential(Credential) || Credential <- Credentials]
    ).

%% @doc Configure a PLAIN server accepting one fixed credential pair.
%%
%% The same validation and pre-use requirement as plain_server/2 applies.
plain_server(Socket, Username, Password) ->
    plain_server(Socket, [{Username, Password}]).

plain_credential({Username, Password}) ->
    {iolist_to_binary(Username), iolist_to_binary(Password)}.

%% @doc Subscribe SUB or XSUB socket to prefix.
subscribe(Socket, Prefix) ->
    omq_nif:subscribe(Socket, iolist_to_binary(Prefix)).

%% @doc Remove SUB or XSUB prefix subscription.
unsubscribe(Socket, Prefix) ->
    omq_nif:unsubscribe(Socket, iolist_to_binary(Prefix)).

%% @doc Join RADIO/DISH group.
join(Socket, Group) ->
    omq_nif:join(Socket, iolist_to_binary(Group)).

%% @doc Leave RADIO/DISH group.
leave(Socket, Group) ->
    omq_nif:leave(Socket, iolist_to_binary(Group)).

%% @doc Send RADIO message to group.
send_group(Socket, Group, Body) ->
    omq_nif:send_group(Socket, iolist_to_binary(Group), iolist_to_binary(Body)).

%% @doc Close socket, optionally with linger in milliseconds.
close(Socket) ->
    close(Socket, 0).

close(Socket, Linger) ->
    clear_socket_process_state(Socket),
    omq_nif:close(Socket, timeout_ms(Linger)).

%% @doc Wait until minimum peer count is connected.
wait_connected(Socket, MinPeers, Timeout) ->
    omq_nif:wait_connected(Socket, MinPeers, timeout_ms(Timeout)).

%% @doc Wait until minimum subscription generation is visible.
wait_subscribed(Socket, MinSubscriptions, Timeout) ->
    omq_nif:wait_subscribed(Socket, MinSubscriptions, timeout_ms(Timeout)).

%% @doc Set socket option. Alias for setsockopt/3.
set(Socket, Option, Value) ->
    setsockopt(Socket, Option, Value).

%% @doc Get socket option. Alias for getsockopt/2.
get(Socket, Option) ->
    getsockopt(Socket, Option).

%% @doc Set socket option by atom or integer option ID.
setsockopt(Socket, hwm, Value) when is_integer(Value) ->
    case setsockopt(Socket, sndhwm, Value) of
        ok -> setsockopt(Socket, rcvhwm, Value);
        Error -> Error
    end;
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

%% @doc Get socket option by atom or integer option ID.
getsockopt(Socket, hwm) ->
    getsockopt(Socket, sndhwm);
getsockopt(Socket, Option) ->
    case option_code(Option) of
        13 -> {ok, rcvmore_value(Socket)};
        Code -> omq_nif:getsockopt(Socket, Code)
    end.

%% @doc Set both SNDHWM and RCVHWM.
set_hwm(Socket, Value) ->
    setsockopt(Socket, hwm, Value).

%% @doc Return SNDHWM as compatibility HWM value.
get_hwm(Socket) ->
    getsockopt(Socket, hwm).

%% @doc Set binary socket option from UTF-8 text.
setsockopt_string(Socket, Option, Text) ->
    setsockopt(Socket, Option, unicode:characters_to_binary(Text)).

%% @doc Get binary socket option as UTF-8 text.
getsockopt_string(Socket, Option) ->
    case getsockopt(Socket, Option) of
        {ok, Data} when is_binary(Data) ->
            {ok, unicode:characters_to_binary(Data)};
        Other ->
            Other
    end.

%% @doc Return socket type atom.
socket_type(Socket) ->
    case omq_nif:socket_type(Socket) of
        {ok, Code} -> {ok, socket_type_atom(Code)};
        Error -> Error
    end.

%% @doc Return wrapper socket ID.
socket_id(Socket) ->
    omq_nif:socket_id(Socket).

%% @doc Return whether socket wrapper is closed.
closed(Socket) ->
    omq_nif:closed(Socket).

timeout_ms(infinity) -> -1;
timeout_ms(Value) when is_integer(Value), Value >= 0 -> Value.

%% Normalize absent routing IDs for multipart metadata.
routing_id(0) -> undefined;
routing_id(Value) -> Value.

bool_int(true) -> 1;
bool_int(false) -> 0.

normalize_poll_entry({Socket, Flags}) -> {Socket, Flags};
normalize_poll_entry(Socket) -> {Socket, pollin()}.

bind_to_random_port_try(_Socket, _Addr, Port, MaxPort) when Port > MaxPort ->
    {error, invalid_endpoint, "no free port"};
bind_to_random_port_try(Socket, Addr, Port, MaxPort) ->
    PortBin = integer_to_binary(Port),
    Endpoint = <<Addr/binary, ":", PortBin/binary>>,
    case bind(Socket, Endpoint) of
        {ok, _Bound} -> {ok, Port};
        _Error -> bind_to_random_port_try(Socket, Addr, Port + 1, MaxPort)
    end.

put_context_instance(Key, IoThreads) ->
    case context(IoThreads) of
        {ok, Context} ->
            persistent_term:put(Key, Context),
            {ok, Context};
        Error ->
            Error
    end.

maybe_clear_context_instance(Context) ->
    Key = {?MODULE, context_instance},
    case persistent_term:get(Key, undefined) of
        Context -> persistent_term:erase(Key);
        _Other -> ok
    end.

version_info_tuple(Version) ->
    [Major, Minor, Patch | _] = binary:split(Version, <<".">>, [global]) ++ [<<"0">>, <<"0">>],
    {leading_integer(Major), leading_integer(Minor), leading_integer(Patch)}.

leading_integer(Bin) ->
    leading_integer(Bin, []).

leading_integer(<<Char, Rest/binary>>, Acc) when Char >= $0, Char =< $9 ->
    leading_integer(Rest, [Char | Acc]);
leading_integer(_Rest, []) ->
    0;
leading_integer(_Rest, Acc) ->
    list_to_integer(lists:reverse(Acc)).

send_options(Flags) when is_integer(Flags) ->
    {0, Flags};
send_options(Opts) ->
    Flags0 = proplists:get_value(flags, Opts, 0),
    Flags = lists:foldl(fun flag_value/2, flag_value(Flags0, 0), Opts),
    {proplists:get_value(routing_id, Opts, 0), Flags}.

flag_value({flags, _}, Acc) -> Acc;
flag_value({routing_id, _}, Acc) -> Acc;
flag_value(sndmore, Acc) -> Acc bor sndmore();
flag_value(noblock, Acc) -> Acc bor noblock();
flag_value(dontwait, Acc) -> Acc bor dontwait();
flag_value({sndmore, true}, Acc) -> Acc bor sndmore();
flag_value({noblock, true}, Acc) -> Acc bor noblock();
flag_value({dontwait, true}, Acc) -> Acc bor dontwait();
flag_value(Value, Acc) when is_integer(Value) -> Acc bor Value;
flag_value(_, Acc) -> Acc.

send_parts(Socket, Parts, RoutingId, Flags) ->
    case (Flags band sndmore()) =/= 0 of
        true ->
            append_send_more(Socket, Parts),
            ok;
        false ->
            FinalParts = take_send_more(Socket) ++ Parts,
            case (Flags band noblock()) =/= 0 of
                true -> omq_nif:try_send(Socket, FinalParts, RoutingId);
                false -> omq_nif:send(Socket, FinalParts, RoutingId)
            end
    end.

try_send_parts(Socket, Parts, RoutingId, Flags) ->
    send_parts(Socket, Parts, RoutingId, Flags bor noblock()).

append_send_more(Socket, Parts) ->
    put(send_more_key(Socket), take_send_more(Socket) ++ Parts).

take_send_more(Socket) ->
    Key = send_more_key(Socket),
    case get(Key) of
        undefined -> [];
        Parts ->
            erase(Key),
            Parts
    end.

send_more_key(Socket) ->
    {?MODULE, sndmore, Socket}.

first_recv_frame(Socket, []) ->
    put(recv_more_key(Socket), []),
    {ok, <<>>};
first_recv_frame(Socket, [Part | Rest]) ->
    put(recv_more_key(Socket), Rest),
    {ok, Part}.

pop_recv_frame(Socket) ->
    Key = recv_more_key(Socket),
    case get(Key) of
        [Part | Rest] ->
            put(Key, Rest),
            {ok, Part};
        [] ->
            erase(Key),
            empty;
        undefined ->
            empty
    end.

recv_more_key(Socket) ->
    {?MODULE, rcvmore, Socket}.

clear_socket_process_state(Socket) ->
    erase(send_more_key(Socket)),
    erase(recv_more_key(Socket)),
    ok.

rcvmore_value(Socket) ->
    case get(recv_more_key(Socket)) of
        [_ | _] -> 1;
        _ -> 0
    end.

decode_term_result({ok, Data}) when is_binary(Data) ->
    decode_term_binary(Data);
decode_term_result({ok, #{data := Data, routing_id := RoutingId}}) when is_binary(Data) ->
    case decode_term_binary(Data) of
        {ok, Term} -> {ok, #{data => Term, routing_id => RoutingId}};
        Error -> Error
    end;
decode_term_result(Other) ->
    Other.

decode_term_binary(Data) ->
    try {ok, binary_to_term(Data, [safe])}
    catch error:badarg -> {error, badarg, "invalid external term format"}
    end.

decode_json_result({ok, Data}) when is_binary(Data) ->
    decode_json_binary(Data);
decode_json_result({ok, #{data := Data, routing_id := RoutingId}}) when is_binary(Data) ->
    case decode_json_binary(Data) of
        {ok, Value} -> {ok, #{data => Value, routing_id => RoutingId}};
        Error -> Error
    end;
decode_json_result(Other) ->
    Other.

decode_json_binary(Data) ->
    try {ok, json:decode(Data)}
    catch error:_ -> {error, badarg, "invalid JSON"}
    end.

poll_ready_entry(InEntries, Index) ->
    {Socket, _Flags} = lists:nth(Index + 1, InEntries),
    {Socket, pollin()}.

merge_poll_ready(Entries) ->
    maps:to_list(lists:foldl(fun({Socket, Flags}, Acc) ->
        maps:update_with(Socket, fun(Existing) -> Existing bor Flags end, Flags, Acc)
    end, #{}, Entries)).

proxy_loop(Frontend, Backend, Capture) ->
    case poll([{Frontend, pollin()}, {Backend, pollin()}], infinity) of
        {ok, Ready} ->
            case lists:member({Frontend, pollin()}, Ready) of
                true -> proxy_forward(Frontend, Backend, Capture);
                false -> ok
            end,
            case lists:member({Backend, pollin()}, Ready) of
                true -> proxy_forward(Backend, Frontend, Capture);
                false -> ok
            end,
            proxy_loop(Frontend, Backend, Capture);
        {error, closed, _} ->
            ok;
        Error ->
            Error
    end.

proxy_forward(In, Out, Capture) ->
    case recv_multipart(In, 0) of
        {ok, Parts} when is_list(Parts) ->
            proxy_capture(Capture, Parts, []),
            send_multipart(Out, Parts);
        {ok, #{parts := Parts, routing_id := RoutingId}} ->
            Opts = [{routing_id, RoutingId}],
            proxy_capture(Capture, Parts, Opts),
            send_multipart(Out, Parts, Opts);
        {error, would_block, _} ->
            ok;
        {error, closed, _} ->
            ok;
        Error ->
            Error
    end.

proxy_capture(undefined, _Parts, _Opts) ->
    ok;
proxy_capture(Capture, Parts, Opts) ->
    _ = send_multipart(Capture, Parts, Opts),
    ok.

proxy_steerable_loop(Frontend, Backend, Capture, Control, State) ->
    Entries = [{Control, pollin()} | proxy_data_poll_entries(Frontend, Backend, State)],
    case poll(Entries, infinity) of
        {ok, Ready} ->
            case lists:member({Control, pollin()}, Ready) of
                true ->
                    case proxy_control_state(Control, State) of
                        terminate -> ok;
                        NextState ->
                            proxy_steerable_forward(Frontend, Backend, Capture, Ready, NextState),
                            proxy_steerable_loop(Frontend, Backend, Capture, Control, NextState)
                    end;
                false ->
                    proxy_steerable_forward(Frontend, Backend, Capture, Ready, State),
                    proxy_steerable_loop(Frontend, Backend, Capture, Control, State)
            end;
        {error, closed, _} ->
            ok;
        Error ->
            Error
    end.

proxy_data_poll_entries(_Frontend, _Backend, paused) ->
    [];
proxy_data_poll_entries(Frontend, Backend, active) ->
    [{Frontend, pollin()}, {Backend, pollin()}].

proxy_control_state(Control, State) ->
    case recv(Control, 0) of
        {ok, <<"TERMINATE">>} -> terminate;
        {ok, <<"PAUSE">>} -> paused;
        {ok, <<"RESUME">>} -> active;
        {ok, _Other} -> State;
        {error, would_block, _} -> State;
        {error, closed, _} -> terminate;
        _Error -> State
    end.

proxy_steerable_forward(_Frontend, _Backend, _Capture, _Ready, paused) ->
    ok;
proxy_steerable_forward(Frontend, Backend, Capture, Ready, active) ->
    case lists:member({Frontend, pollin()}, Ready) of
        true -> proxy_forward(Frontend, Backend, Capture);
        false -> ok
    end,
    case lists:member({Backend, pollin()}, Ready) of
        true -> proxy_forward(Backend, Frontend, Capture);
        false -> ok
    end.

%% @doc Return POLLIN constant.
pollin() -> 1.
%% @doc Return POLLOUT constant.
pollout() -> 2.
%% @doc Return POLLERR constant.
pollerr() -> 4.
%% @doc Return POLLPRI constant.
pollpri() -> 32.
%% @doc Return SNDMORE constant.
sndmore() -> 2.
%% @doc Return NOBLOCK constant.
noblock() -> 1.
%% @doc Return DONTWAIT constant.
dontwait() -> noblock().

%% @doc Return HWM constant.
hwm() -> 1.

%% @doc Return PAIR constant.
pair() -> 0.
%% @doc Return PUB socket type constant.
pub() -> 1.
%% @doc Return SUB socket type constant.
sub() -> 2.
%% @doc Return REQ socket type constant.
req() -> 3.
%% @doc Return REP socket type constant.
rep() -> 4.
%% @doc Return DEALER constant.
dealer() -> 5.
%% @doc Return ROUTER constant.
router() -> 6.
%% @doc Return PULL constant.
pull() -> 7.
%% @doc Return PUSH constant.
push() -> 8.
%% @doc Return XPUB constant.
xpub() -> 9.
%% @doc Return XSUB constant.
xsub() -> 10.
%% @doc Return STREAM constant.
stream() -> 11.
%% @doc Return SERVER constant.
server() -> 12.
%% @doc Return CLIENT constant.
client() -> 13.
%% @doc Return RADIO constant.
radio() -> 14.
%% @doc Return DISH constant.
dish() -> 15.
%% @doc Return GATHER constant.
gather() -> 16.
%% @doc Return SCATTER constant.
scatter() -> 17.
%% @doc Return PEER constant.
peer() -> 19.
%% @doc Return CHANNEL constant.
channel() -> 20.

%% @doc Return AFFINITY constant.
affinity() -> 4.
%% @doc Return IDENTITY constant.
identity() -> 5.
%% @doc Return ROUTING_ID constant.
routing_id() -> 5.
%% @doc Return SUBSCRIBE_OPT constant.
subscribe_opt() -> 6.
%% @doc Return UNSUBSCRIBE_OPT constant.
unsubscribe_opt() -> 7.
%% @doc Return RCVMORE constant.
rcvmore() -> 13.
%% @doc Return FD constant.
fd() -> 14.
%% @doc Return EVENTS constant.
events() -> 15.
%% @doc Return TYPE constant.
type() -> 16.
%% @doc Return LINGER constant.
linger() -> 17.
%% @doc Return RECONNECT_IVL constant.
reconnect_ivl() -> 18.
%% @doc Return BACKLOG constant.
backlog() -> 19.
%% @doc Return RECONNECT_IVL_MAX constant.
reconnect_ivl_max() -> 21.
%% @doc Return MAXMSGSIZE constant.
maxmsgsize() -> 22.
%% @doc Return SNDHWM constant.
sndhwm() -> 23.
%% @doc Return RCVHWM constant.
rcvhwm() -> 24.
%% @doc Return RCVTIMEO constant.
rcvtimeo() -> 27.
%% @doc Return SNDTIMEO constant.
sndtimeo() -> 28.
%% @doc Return ROUTER_MANDATORY constant.
router_mandatory() -> 33.
%% @doc Return TCP_KEEPALIVE constant.
tcp_keepalive() -> 34.
%% @doc Return TCP_KEEPALIVE_CNT constant.
tcp_keepalive_cnt() -> 35.
%% @doc Return TCP_KEEPALIVE_IDLE constant.
tcp_keepalive_idle() -> 36.
%% @doc Return TCP_KEEPALIVE_INTVL constant.
tcp_keepalive_intvl() -> 37.
%% @doc Return IMMEDIATE constant.
immediate() -> 39.
%% @doc Return IPV6 constant.
ipv6() -> 42.
%% @doc Return MECHANISM constant.
mechanism() -> 43.
%% @doc Return PLAIN_SERVER constant.
plain_server() -> 44.
%% @doc Return PLAIN_USERNAME constant.
plain_username() -> 45.
%% @doc Return PLAIN_PASSWORD constant.
plain_password() -> 46.
%% @doc Return CURVE_SERVER constant.
curve_server() -> 47.
%% @doc Return CURVE_PUBLICKEY constant.
curve_publickey() -> 48.
%% @doc Return CURVE_SECRETKEY constant.
curve_secretkey() -> 49.
%% @doc Return CURVE_SERVERKEY constant.
curve_serverkey() -> 50.
%% @doc Return CONFLATE constant.
conflate() -> 54.
%% @doc Return ROUTER_HANDOVER constant.
router_handover() -> 56.
%% @doc Return HANDSHAKE_IVL constant.
handshake_ivl() -> 66.
%% @doc Return HEARTBEAT_IVL constant.
heartbeat_ivl() -> 75.
%% @doc Return HEARTBEAT_TTL constant.
heartbeat_ttl() -> 76.
%% @doc Return HEARTBEAT_TIMEOUT constant.
heartbeat_timeout() -> 77.
%% @doc Return CONNECT_TIMEOUT constant.
connect_timeout() -> 79.
%% @doc Return TCP_MAXRT constant.
tcp_maxrt() -> 80.
%% @doc Return RECONNECT_STOP constant.
reconnect_stop() -> 109.
%% @doc Return RATE constant.
rate() -> 8.
%% @doc Return SNDBUF constant.
sndbuf() -> 11.
%% @doc Return RCVBUF constant.
rcvbuf() -> 12.
%% @doc Return LAST_ENDPOINT constant.
last_endpoint() -> 32.
%% @doc Return IPV4ONLY constant.
ipv4only() -> 31.
%% @doc Return TCP_ACCEPT_FILTER constant.
tcp_accept_filter() -> 38.
%% @doc Return XPUB_VERBOSE constant.
xpub_verbose() -> 40.
%% @doc Return PROBE_ROUTER constant.
probe_router() -> 51.
%% @doc Return REQ_CORRELATE constant.
req_correlate() -> 52.
%% @doc Return REQ_RELAXED constant.
req_relaxed() -> 53.
%% @doc Return ZAP_DOMAIN constant.
zap_domain() -> 55.
%% @doc Return MULTICAST_HOPS constant.
multicast_hops() -> 25.
%% @doc Return RECOVERY_IVL constant.
recovery_ivl() -> 9.
%% @doc Return OMQ_ON_MUTE constant.
omq_on_mute() -> 1004.
%% @doc Return OMQ_COMPRESSION_LEVEL constant.
omq_compression_level() -> 1005.
%% @doc Return OMQ_COMPRESSION_DICT constant.
omq_compression_dict() -> 1006.
%% @doc Return OMQ_COMPRESSION_AUTO_TRAIN constant.
omq_compression_auto_train() -> 1007.
%% @doc Return OMQ_WORKLOAD_PROFILE constant.
omq_workload_profile() -> 1100.
%% @doc Return OMQ_ON_MUTE_BLOCK constant.
omq_on_mute_block() -> 0.
%% @doc Return OMQ_ON_MUTE_DROP_NEWEST constant.
omq_on_mute_drop_newest() -> 1.
%% @doc Return OMQ_ON_MUTE_DROP_OLDEST constant.
omq_on_mute_drop_oldest() -> 2.
%% @doc Return FORWARDER constant.
forwarder() -> 2.
%% @doc Return QUEUE constant.
queue() -> 3.
%% @doc Return STREAMER constant.
streamer() -> 1.
%% @doc Return NULL constant.
null() -> 0.
%% @doc Return PLAIN constant.
plain() -> 1.
%% @doc Return CURVE constant.
curve() -> 2.

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

errno_atom(11) -> eagain;
errno_atom(95) -> enotsup;
errno_atom(22) -> einval;
errno_atom(14) -> efault;
errno_atom(12) -> enomem;
errno_atom(19) -> enodev;
errno_atom(90) -> emsgsize;
errno_atom(97) -> eafnosupport;
errno_atom(101) -> enetunreach;
errno_atom(103) -> econnaborted;
errno_atom(104) -> econnreset;
errno_atom(107) -> enotconn;
errno_atom(110) -> etimedout;
errno_atom(113) -> ehostunreach;
errno_atom(102) -> enetreset;
errno_atom(98) -> eaddrinuse;
errno_atom(99) -> eaddrnotavail;
errno_atom(108) -> enotsock;
errno_atom(_Errno) -> undefined.

option_code(Option) when is_integer(Option) -> Option;
option_code(hwm) -> hwm();
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
