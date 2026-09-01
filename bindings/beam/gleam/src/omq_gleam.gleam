//// Gleam API for OMQ.
////
//// OMQ sockets are ZeroMQ-compatible message queues backed by the Rust
//// `omq-tokio` runtime. Create a context with `context` or reuse the
//// process-wide singleton with `context_instance`, then create sockets with
//// `socket`.
////
//// Endpoints are UTF-8 bit arrays such as <<"tcp://127.0.0.1:5555":utf8>>,
//// <<"ipc:///tmp/omq.sock":utf8>>, <<"inproc://queue":utf8>>,
//// <<"lz4+tcp://127.0.0.1:5555":utf8>>, and
//// <<"zstd+tcp://127.0.0.1:5555":utf8>>.
////
//// Functions return `Ok(value)` or `Error(#(class, reason))` unless they are
//// pure constants or feature checks. `send` and `recv` move binary frames;
//// string, JSON, term, multipart, pub/sub, RADIO/DISH, and socket option
//// helpers wrap the Erlang base package.

import gleam/dynamic.{type Dynamic}

/// Native OMQ context resource.
pub type Context

/// Native OMQ socket resource.
pub type Socket

/// Socket monitor event stream resource.
pub type Monitor

/// Opaque monitor event map from Erlang.
pub type MonitorEvent

/// Opaque connection snapshot map from Erlang.
pub type ConnectionInfo

/// Error tuple with class and reason.
pub type Error =
  #(String, String)

/// Create a context with one IO thread.
@external(erlang, "omq_gleam_ffi", "context")
pub fn context() -> Result(Context, Error)

/// Return process-wide singleton context.
@external(erlang, "omq_gleam_ffi", "context_instance")
pub fn context_instance() -> Result(Context, Error)

/// Return process-wide singleton context, creating it with IO thread count.
@external(erlang, "omq_gleam_ffi", "context_instance")
pub fn context_instance_with_io_threads(
  io_threads: Int,
) -> Result(Context, Error)

/// Return process-wide singleton context.
@external(erlang, "omq_gleam_ffi", "instance")
pub fn instance() -> Result(Context, Error)

/// Return process-wide singleton context, creating it with IO thread count.
@external(erlang, "omq_gleam_ffi", "instance")
pub fn instance_with_io_threads(io_threads: Int) -> Result(Context, Error)

/// Terminate a context.
@external(erlang, "omq_gleam_ffi", "term")
pub fn term(context: Context) -> Result(Nil, Error)

/// Terminate a context. Alias for term.
@external(erlang, "omq_gleam_ffi", "destroy")
pub fn destroy(context: Context) -> Result(Nil, Error)

/// Return opaque native context share key.
@external(erlang, "omq_gleam_ffi", "context_share_key")
pub fn context_share_key(context: Context) -> Result(Int, Error)

/// Import native context by share key.
@external(erlang, "omq_gleam_ffi", "context_from_share_key")
pub fn context_from_share_key(share_key: Int) -> Result(Context, Error)

/// Return whether context wrapper or native core is closed.
@external(erlang, "omq_gleam_ffi", "context_closed")
pub fn context_closed(context: Context) -> Bool

/// Return native backend name.
@external(erlang, "omq_gleam_ffi", "backend_name")
pub fn backend_name() -> Result(BitArray, Error)

/// Return native binding version.
@external(erlang, "omq_gleam_ffi", "version")
pub fn version() -> Result(BitArray, Error)

/// Return native binding version. Alias for version.
@external(erlang, "omq_gleam_ffi", "omq_version")
pub fn omq_version() -> Result(BitArray, Error)

/// Return native binding version as #(major, minor, patch).
@external(erlang, "omq_gleam_ffi", "omq_version_info")
pub fn omq_version_info() -> Result(#(Int, Int, Int), Error)

/// Return libzmq compatibility version string.
@external(erlang, "omq_gleam_ffi", "zmq_version")
pub fn zmq_version() -> BitArray

/// Return libzmq compatibility version tuple.
@external(erlang, "omq_gleam_ffi", "zmq_version_info")
pub fn zmq_version_info() -> #(Int, Int, Int)

/// Return POSIX strerror text for common libzmq errno values.
@external(erlang, "omq_gleam_ffi", "strerror")
pub fn strerror(errno: Int) -> BitArray

/// Return opaque native context share key.
@external(erlang, "omq_gleam_ffi", "share_key")
pub fn share_key(context: Context) -> Result(Int, Error)

/// Import native context by share key.
@external(erlang, "omq_gleam_ffi", "from_share_key")
pub fn from_share_key(share_key: Int) -> Result(Context, Error)

/// Create socket from context and socket type constant.
@external(erlang, "omq_gleam_ffi", "socket")
pub fn socket(context: Context, socket_type: Int) -> Result(Socket, Error)

/// Bind socket to endpoint and return bound endpoint.
@external(erlang, "omq_gleam_ffi", "bind")
pub fn bind(socket: Socket, endpoint: BitArray) -> Result(BitArray, Error)

/// Bind socket to random port in inclusive range.
@external(erlang, "omq_gleam_ffi", "bind_to_random_port")
pub fn bind_to_random_port(
  socket: Socket,
  addr: BitArray,
  min_port: Int,
  max_port: Int,
) -> Result(Int, Error)

/// Connect socket to endpoint.
@external(erlang, "omq_gleam_ffi", "connect")
pub fn connect(socket: Socket, endpoint: BitArray) -> Result(Nil, Error)

/// Unbind socket from endpoint.
@external(erlang, "omq_gleam_ffi", "unbind")
pub fn unbind(socket: Socket, endpoint: BitArray) -> Result(Nil, Error)

/// Disconnect socket from endpoint.
@external(erlang, "omq_gleam_ffi", "disconnect")
pub fn disconnect(socket: Socket, endpoint: BitArray) -> Result(Nil, Error)

/// Create monitor stream for socket lifecycle events.
@external(erlang, "omq_gleam_ffi", "monitor")
pub fn monitor(socket: Socket) -> Result(Monitor, Error)

/// Receive next monitor event with timeout in milliseconds.
@external(erlang, "omq_gleam_ffi", "monitor_recv")
pub fn monitor_recv(
  monitor: Monitor,
  timeout_ms: Int,
) -> Result(MonitorEvent, Error)

/// Try to receive one monitor event without blocking.
@external(erlang, "omq_gleam_ffi", "monitor_try_recv")
pub fn monitor_try_recv(monitor: Monitor) -> Result(MonitorEvent, Error)

/// Return current connection snapshots for socket.
@external(erlang, "omq_gleam_ffi", "connections")
pub fn connections(socket: Socket) -> Result(List(ConnectionInfo), Error)

/// Return one connection snapshot by ID.
@external(erlang, "omq_gleam_ffi", "connection_info")
pub fn connection_info(
  socket: Socket,
  connection_id: Int,
) -> Result(ConnectionInfo, Error)

/// Run bidirectional proxy between two sockets.
@external(erlang, "omq_gleam_ffi", "proxy")
pub fn proxy(frontend: Socket, backend: Socket) -> Result(Nil, Error)

/// Run proxy and mirror traffic to capture socket.
@external(erlang, "omq_gleam_ffi", "proxy_with_capture")
pub fn proxy_with_capture(
  frontend: Socket,
  backend: Socket,
  capture: Socket,
) -> Result(Nil, Error)

/// Run steerable proxy with PAUSE, RESUME, and TERMINATE control.
@external(erlang, "omq_gleam_ffi", "proxy_steerable")
pub fn proxy_steerable(
  frontend: Socket,
  backend: Socket,
  capture: Socket,
  control: Socket,
) -> Result(Nil, Error)

/// Run libzmq-compatible device. Device type is accepted for parity.
@external(erlang, "omq_gleam_ffi", "device")
pub fn device(
  device_type: Int,
  frontend: Socket,
  backend: Socket,
) -> Result(Nil, Error)

/// Send one binary message.
@external(erlang, "omq_gleam_ffi", "send")
pub fn send(socket: Socket, data: BitArray) -> Result(Nil, Error)

/// Send one UTF-8 string message.
@external(erlang, "omq_gleam_ffi", "send_string")
pub fn send_string(socket: Socket, text: String) -> Result(Nil, Error)

/// Send one JSON value encoded by OTP `json`.
@external(erlang, "omq_gleam_ffi", "send_json")
pub fn send_json(socket: Socket, value: Dynamic) -> Result(Nil, Error)

/// Send one Erlang term using external term format.
@external(erlang, "omq_gleam_ffi", "send_term")
pub fn send_term(socket: Socket, term: Dynamic) -> Result(Nil, Error)

/// Send one multipart message.
@external(erlang, "omq_gleam_ffi", "send_multipart")
pub fn send_multipart(
  socket: Socket,
  parts: List(BitArray),
) -> Result(Nil, Error)

/// Receive one binary message.
@external(erlang, "omq_gleam_ffi", "recv")
pub fn recv(socket: Socket) -> Result(BitArray, Error)

/// Receive one UTF-8 string message.
@external(erlang, "omq_gleam_ffi", "recv_string")
pub fn recv_string(socket: Socket) -> Result(String, Error)

/// Receive one string with timeout in milliseconds.
@external(erlang, "omq_gleam_ffi", "recv_string_timeout")
pub fn recv_string_timeout(
  socket: Socket,
  timeout_ms: Int,
) -> Result(String, Error)

/// Receive one JSON value decoded by OTP `json`.
@external(erlang, "omq_gleam_ffi", "recv_json")
pub fn recv_json(socket: Socket) -> Result(Dynamic, Error)

/// Try to receive one JSON value without blocking.
@external(erlang, "omq_gleam_ffi", "try_recv_json")
pub fn try_recv_json(socket: Socket) -> Result(Dynamic, Error)

/// Receive one Erlang term encoded by `send_term`.
@external(erlang, "omq_gleam_ffi", "recv_term")
pub fn recv_term(socket: Socket) -> Result(Dynamic, Error)

/// Try to receive one Erlang term without blocking.
@external(erlang, "omq_gleam_ffi", "try_recv_term")
pub fn try_recv_term(socket: Socket) -> Result(Dynamic, Error)

/// Receive next frame from multipart message.
@external(erlang, "omq_gleam_ffi", "recv_frame")
pub fn recv_frame(socket: Socket) -> Result(BitArray, Error)

/// Try to receive one binary message without blocking.
@external(erlang, "omq_gleam_ffi", "try_recv")
pub fn try_recv(socket: Socket) -> Result(BitArray, Error)

/// Try to receive one UTF-8 string without blocking.
@external(erlang, "omq_gleam_ffi", "try_recv_string")
pub fn try_recv_string(socket: Socket) -> Result(String, Error)

/// Receive one multipart message.
@external(erlang, "omq_gleam_ffi", "recv_multipart")
pub fn recv_multipart(socket: Socket) -> Result(List(BitArray), Error)

/// Try to receive one multipart message without blocking.
@external(erlang, "omq_gleam_ffi", "try_recv_multipart")
pub fn try_recv_multipart(socket: Socket) -> Result(List(BitArray), Error)

/// Subscribe SUB or XSUB socket to prefix.
@external(erlang, "omq_gleam_ffi", "subscribe")
pub fn subscribe(socket: Socket, prefix: BitArray) -> Result(Nil, Error)

/// Remove SUB or XSUB prefix subscription.
@external(erlang, "omq_gleam_ffi", "unsubscribe")
pub fn unsubscribe(socket: Socket, prefix: BitArray) -> Result(Nil, Error)

/// Join RADIO/DISH group.
@external(erlang, "omq_gleam_ffi", "join")
pub fn join(socket: Socket, group: BitArray) -> Result(Nil, Error)

/// Leave RADIO/DISH group.
@external(erlang, "omq_gleam_ffi", "leave")
pub fn leave(socket: Socket, group: BitArray) -> Result(Nil, Error)

/// Send RADIO message to group.
@external(erlang, "omq_gleam_ffi", "send_group")
pub fn send_group(
  socket: Socket,
  group: BitArray,
  body: BitArray,
) -> Result(Nil, Error)

/// Close socket with zero linger.
@external(erlang, "omq_gleam_ffi", "close")
pub fn close(socket: Socket) -> Result(Nil, Error)

/// Wait until minimum peer count is connected.
@external(erlang, "omq_gleam_ffi", "wait_connected")
pub fn wait_connected(
  socket: Socket,
  min_peers: Int,
  timeout_ms: Int,
) -> Result(Int, Error)

/// Wait until minimum subscription generation is visible.
@external(erlang, "omq_gleam_ffi", "wait_subscribed")
pub fn wait_subscribed(
  socket: Socket,
  min_subscriptions: Int,
  timeout_ms: Int,
) -> Result(Int, Error)

/// Set integer socket option.
@external(erlang, "omq_gleam_ffi", "setsockopt_int")
pub fn setsockopt_int(
  socket: Socket,
  option: Int,
  value: Int,
) -> Result(Nil, Error)

/// Set binary socket option.
@external(erlang, "omq_gleam_ffi", "setsockopt_binary")
pub fn setsockopt_binary(
  socket: Socket,
  option: Int,
  value: BitArray,
) -> Result(Nil, Error)

/// Set binary socket option from UTF-8 string.
@external(erlang, "omq_gleam_ffi", "setsockopt_string")
pub fn setsockopt_string(
  socket: Socket,
  option: Int,
  value: String,
) -> Result(Nil, Error)

/// Get integer socket option.
@external(erlang, "omq_gleam_ffi", "getsockopt_int")
pub fn getsockopt_int(socket: Socket, option: Int) -> Result(Int, Error)

/// Get binary socket option.
@external(erlang, "omq_gleam_ffi", "getsockopt_binary")
pub fn getsockopt_binary(socket: Socket, option: Int) -> Result(BitArray, Error)

/// Get binary socket option as UTF-8 string.
@external(erlang, "omq_gleam_ffi", "getsockopt_string")
pub fn getsockopt_string(socket: Socket, option: Int) -> Result(String, Error)

/// Set both SNDHWM and RCVHWM.
@external(erlang, "omq_gleam_ffi", "set_hwm")
pub fn set_hwm(socket: Socket, value: Int) -> Result(Nil, Error)

/// Return SNDHWM as compatibility HWM value.
@external(erlang, "omq_gleam_ffi", "get_hwm")
pub fn get_hwm(socket: Socket) -> Result(Int, Error)

/// Return socket type atom name as string.
@external(erlang, "omq_gleam_ffi", "socket_type")
pub fn socket_type(socket: Socket) -> Result(String, Error)

/// Return wrapper socket ID.
@external(erlang, "omq_gleam_ffi", "socket_id")
pub fn socket_id(socket: Socket) -> Result(Int, Error)

/// Return whether socket wrapper is closed.
@external(erlang, "omq_gleam_ffi", "closed")
pub fn closed(socket: Socket) -> Bool

/// Return whether native feature or transport is available.
@external(erlang, "omq_gleam_ffi", "has")
pub fn has(capability: BitArray) -> Bool

/// Generate CURVE public/secret keypair.
@external(erlang, "omq_gleam_ffi", "curve_keypair")
pub fn curve_keypair() -> Result(#(BitArray, BitArray), Error)

/// Derive CURVE public key from secret key.
@external(erlang, "omq_gleam_ffi", "curve_public")
pub fn curve_public(secret: BitArray) -> Result(BitArray, Error)

/// Return POLLIN constant.
pub fn pollin() -> Int {
  1
}

/// Return POLLOUT constant.
pub fn pollout() -> Int {
  2
}

/// Return POLLERR constant.
pub fn pollerr() -> Int {
  4
}

/// Return POLLPRI constant.
pub fn pollpri() -> Int {
  32
}

/// Return SNDMORE constant.
pub fn sndmore() -> Int {
  2
}

/// Return NOBLOCK constant.
pub fn noblock() -> Int {
  1
}

/// Return DONTWAIT constant.
pub fn dontwait() -> Int {
  1
}

/// Return HWM constant.
pub fn hwm() -> Int {
  1
}

/// Return PAIR constant.
pub fn pair() -> Int {
  0
}

/// Return PUB socket type constant.
pub fn publisher() -> Int {
  1
}

/// Return SUB socket type constant.
pub fn subscriber() -> Int {
  2
}

/// Return REQ constant.
pub fn req() -> Int {
  3
}

/// Return REP constant.
pub fn rep() -> Int {
  4
}

/// Return DEALER constant.
pub fn dealer() -> Int {
  5
}

/// Return ROUTER constant.
pub fn router() -> Int {
  6
}

/// Return PULL constant.
pub fn pull() -> Int {
  7
}

/// Return PUSH constant.
pub fn push() -> Int {
  8
}

/// Return XPUB constant.
pub fn xpub() -> Int {
  9
}

/// Return XSUB constant.
pub fn xsub() -> Int {
  10
}

/// Return STREAM constant.
pub fn stream() -> Int {
  11
}

/// Return SERVER constant.
pub fn server() -> Int {
  12
}

/// Return CLIENT constant.
pub fn client() -> Int {
  13
}

/// Return RADIO constant.
pub fn radio() -> Int {
  14
}

/// Return DISH constant.
pub fn dish() -> Int {
  15
}

/// Return GATHER constant.
pub fn gather() -> Int {
  16
}

/// Return SCATTER constant.
pub fn scatter() -> Int {
  17
}

/// Return PEER constant.
pub fn peer() -> Int {
  19
}

/// Return CHANNEL constant.
pub fn channel() -> Int {
  20
}

/// Return AFFINITY constant.
pub fn affinity() -> Int {
  4
}

/// Return IDENTITY constant.
pub fn identity() -> Int {
  5
}

/// Return ROUTING_ID constant.
pub fn routing_id() -> Int {
  5
}

/// Return SUBSCRIBE option ID.
pub fn subscribe_opt() -> Int {
  6
}

/// Return UNSUBSCRIBE option ID.
pub fn unsubscribe_opt() -> Int {
  7
}

/// Return RATE constant.
pub fn rate() -> Int {
  8
}

/// Return RECOVERY_IVL constant.
pub fn recovery_ivl() -> Int {
  9
}

/// Return SNDBUF constant.
pub fn sndbuf() -> Int {
  11
}

/// Return RCVBUF constant.
pub fn rcvbuf() -> Int {
  12
}

/// Return RCVMORE constant.
pub fn rcvmore() -> Int {
  13
}

/// Return FD constant.
pub fn fd() -> Int {
  14
}

/// Return EVENTS constant.
pub fn events() -> Int {
  15
}

/// Return TYPE option ID.
pub fn type_option() -> Int {
  16
}

/// Return LINGER constant.
pub fn linger() -> Int {
  17
}

/// Return RECONNECT_IVL constant.
pub fn reconnect_ivl() -> Int {
  18
}

/// Return BACKLOG constant.
pub fn backlog() -> Int {
  19
}

/// Return RECONNECT_IVL_MAX constant.
pub fn reconnect_ivl_max() -> Int {
  21
}

/// Return MAXMSGSIZE constant.
pub fn maxmsgsize() -> Int {
  22
}

/// Return SNDHWM constant.
pub fn sndhwm() -> Int {
  23
}

/// Return RCVHWM constant.
pub fn rcvhwm() -> Int {
  24
}

/// Return MULTICAST_HOPS constant.
pub fn multicast_hops() -> Int {
  25
}

/// Return RCVTIMEO constant.
pub fn rcvtimeo() -> Int {
  27
}

/// Return SNDTIMEO constant.
pub fn sndtimeo() -> Int {
  28
}

/// Return IPV4ONLY constant.
pub fn ipv4only() -> Int {
  31
}

/// Return LAST_ENDPOINT constant.
pub fn last_endpoint() -> Int {
  32
}

/// Return ROUTER_MANDATORY constant.
pub fn router_mandatory() -> Int {
  33
}

/// Return TCP_KEEPALIVE constant.
pub fn tcp_keepalive() -> Int {
  34
}

/// Return TCP_KEEPALIVE_CNT constant.
pub fn tcp_keepalive_cnt() -> Int {
  35
}

/// Return TCP_KEEPALIVE_IDLE constant.
pub fn tcp_keepalive_idle() -> Int {
  36
}

/// Return TCP_KEEPALIVE_INTVL constant.
pub fn tcp_keepalive_intvl() -> Int {
  37
}

/// Return TCP_ACCEPT_FILTER constant.
pub fn tcp_accept_filter() -> Int {
  38
}

/// Return IMMEDIATE constant.
pub fn immediate() -> Int {
  39
}

/// Return XPUB_VERBOSE constant.
pub fn xpub_verbose() -> Int {
  40
}

/// Return IPV6 constant.
pub fn ipv6() -> Int {
  42
}

/// Return MECHANISM constant.
pub fn mechanism() -> Int {
  43
}

/// Return PLAIN_SERVER constant.
pub fn plain_server() -> Int {
  44
}

/// Return PLAIN_USERNAME constant.
pub fn plain_username() -> Int {
  45
}

/// Return PLAIN_PASSWORD constant.
pub fn plain_password() -> Int {
  46
}

/// Return CURVE_SERVER constant.
pub fn curve_server() -> Int {
  47
}

/// Return CURVE_PUBLICKEY constant.
pub fn curve_publickey() -> Int {
  48
}

/// Return CURVE_SECRETKEY constant.
pub fn curve_secretkey() -> Int {
  49
}

/// Return CURVE_SERVERKEY constant.
pub fn curve_serverkey() -> Int {
  50
}

/// Return PROBE_ROUTER constant.
pub fn probe_router() -> Int {
  51
}

/// Return REQ_CORRELATE constant.
pub fn req_correlate() -> Int {
  52
}

/// Return REQ_RELAXED constant.
pub fn req_relaxed() -> Int {
  53
}

/// Return CONFLATE constant.
pub fn conflate() -> Int {
  54
}

/// Return ZAP_DOMAIN constant.
pub fn zap_domain() -> Int {
  55
}

/// Return ROUTER_HANDOVER constant.
pub fn router_handover() -> Int {
  56
}

/// Return HANDSHAKE_IVL constant.
pub fn handshake_ivl() -> Int {
  66
}

/// Return HEARTBEAT_IVL constant.
pub fn heartbeat_ivl() -> Int {
  75
}

/// Return HEARTBEAT_TTL constant.
pub fn heartbeat_ttl() -> Int {
  76
}

/// Return HEARTBEAT_TIMEOUT constant.
pub fn heartbeat_timeout() -> Int {
  77
}

/// Return CONNECT_TIMEOUT constant.
pub fn connect_timeout() -> Int {
  79
}

/// Return TCP_MAXRT constant.
pub fn tcp_maxrt() -> Int {
  80
}

/// Return RECONNECT_STOP constant.
pub fn reconnect_stop() -> Int {
  109
}

/// Return OMQ_ON_MUTE constant.
pub fn omq_on_mute() -> Int {
  1004
}

/// Return OMQ_COMPRESSION_LEVEL constant.
pub fn omq_compression_level() -> Int {
  1005
}

/// Return OMQ_COMPRESSION_DICT constant.
pub fn omq_compression_dict() -> Int {
  1006
}

/// Return OMQ_COMPRESSION_AUTO_TRAIN constant.
pub fn omq_compression_auto_train() -> Int {
  1007
}

/// Return OMQ_WORKLOAD_PROFILE constant.
pub fn omq_workload_profile() -> Int {
  1100
}

/// Return OMQ_ON_MUTE block mode value.
pub fn omq_on_mute_block() -> Int {
  0
}

/// Return OMQ_ON_MUTE drop-newest mode value.
pub fn omq_on_mute_drop_newest() -> Int {
  1
}

/// Return OMQ_ON_MUTE drop-oldest mode value.
pub fn omq_on_mute_drop_oldest() -> Int {
  2
}

/// Return FORWARDER constant.
pub fn forwarder() -> Int {
  2
}

/// Return QUEUE constant.
pub fn queue() -> Int {
  3
}

/// Return STREAMER constant.
pub fn streamer() -> Int {
  1
}

/// Return NULL constant.
pub fn null() -> Int {
  0
}

/// Return PLAIN constant.
pub fn plain() -> Int {
  1
}

/// Return CURVE constant.
pub fn curve() -> Int {
  2
}
