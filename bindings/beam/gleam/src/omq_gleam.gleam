pub type Context

pub type Socket

pub type Error =
  #(String, String)

@external(erlang, "omq_gleam_ffi", "context")
pub fn context() -> Result(Context, Error)

@external(erlang, "omq_gleam_ffi", "term")
pub fn term(context: Context) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "socket")
pub fn socket(context: Context, socket_type: Int) -> Result(Socket, Error)

@external(erlang, "omq_gleam_ffi", "bind")
pub fn bind(socket: Socket, endpoint: BitArray) -> Result(BitArray, Error)

@external(erlang, "omq_gleam_ffi", "bind_to_random_port")
pub fn bind_to_random_port(
  socket: Socket,
  addr: BitArray,
  min_port: Int,
  max_port: Int,
) -> Result(Int, Error)

@external(erlang, "omq_gleam_ffi", "connect")
pub fn connect(socket: Socket, endpoint: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "unbind")
pub fn unbind(socket: Socket, endpoint: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "disconnect")
pub fn disconnect(socket: Socket, endpoint: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "send")
pub fn send(socket: Socket, data: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "send_multipart")
pub fn send_multipart(
  socket: Socket,
  parts: List(BitArray),
) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "recv")
pub fn recv(socket: Socket) -> Result(BitArray, Error)

@external(erlang, "omq_gleam_ffi", "recv_frame")
pub fn recv_frame(socket: Socket) -> Result(BitArray, Error)

@external(erlang, "omq_gleam_ffi", "try_recv")
pub fn try_recv(socket: Socket) -> Result(BitArray, Error)

@external(erlang, "omq_gleam_ffi", "recv_multipart")
pub fn recv_multipart(socket: Socket) -> Result(List(BitArray), Error)

@external(erlang, "omq_gleam_ffi", "try_recv_multipart")
pub fn try_recv_multipart(socket: Socket) -> Result(List(BitArray), Error)

@external(erlang, "omq_gleam_ffi", "subscribe")
pub fn subscribe(socket: Socket, prefix: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "unsubscribe")
pub fn unsubscribe(socket: Socket, prefix: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "join")
pub fn join(socket: Socket, group: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "leave")
pub fn leave(socket: Socket, group: BitArray) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "send_group")
pub fn send_group(
  socket: Socket,
  group: BitArray,
  body: BitArray,
) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "close")
pub fn close(socket: Socket) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "wait_connected")
pub fn wait_connected(
  socket: Socket,
  min_peers: Int,
  timeout_ms: Int,
) -> Result(Int, Error)

@external(erlang, "omq_gleam_ffi", "wait_subscribed")
pub fn wait_subscribed(
  socket: Socket,
  min_subscriptions: Int,
  timeout_ms: Int,
) -> Result(Int, Error)

@external(erlang, "omq_gleam_ffi", "setsockopt_int")
pub fn setsockopt_int(
  socket: Socket,
  option: Int,
  value: Int,
) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "setsockopt_binary")
pub fn setsockopt_binary(
  socket: Socket,
  option: Int,
  value: BitArray,
) -> Result(Nil, Error)

@external(erlang, "omq_gleam_ffi", "getsockopt_int")
pub fn getsockopt_int(socket: Socket, option: Int) -> Result(Int, Error)

@external(erlang, "omq_gleam_ffi", "getsockopt_binary")
pub fn getsockopt_binary(socket: Socket, option: Int) -> Result(BitArray, Error)

@external(erlang, "omq_gleam_ffi", "socket_type")
pub fn socket_type(socket: Socket) -> Result(String, Error)

@external(erlang, "omq_gleam_ffi", "has")
pub fn has(capability: BitArray) -> Bool

pub fn pollin() -> Int {
  1
}

pub fn pollout() -> Int {
  2
}

pub fn pollerr() -> Int {
  4
}

pub fn sndmore() -> Int {
  2
}

pub fn noblock() -> Int {
  1
}

pub fn dontwait() -> Int {
  1
}

pub fn pair() -> Int {
  0
}

pub fn publisher() -> Int {
  1
}

pub fn subscriber() -> Int {
  2
}

pub fn req() -> Int {
  3
}

pub fn rep() -> Int {
  4
}

pub fn dealer() -> Int {
  5
}

pub fn router() -> Int {
  6
}

pub fn pull() -> Int {
  7
}

pub fn push() -> Int {
  8
}

pub fn xpub() -> Int {
  9
}

pub fn xsub() -> Int {
  10
}

pub fn stream() -> Int {
  11
}

pub fn server() -> Int {
  12
}

pub fn client() -> Int {
  13
}

pub fn radio() -> Int {
  14
}

pub fn dish() -> Int {
  15
}

pub fn gather() -> Int {
  16
}

pub fn scatter() -> Int {
  17
}

pub fn peer() -> Int {
  19
}

pub fn channel() -> Int {
  20
}

pub fn affinity() -> Int {
  4
}

pub fn identity() -> Int {
  5
}

pub fn routing_id() -> Int {
  5
}

pub fn subscribe_opt() -> Int {
  6
}

pub fn unsubscribe_opt() -> Int {
  7
}

pub fn rate() -> Int {
  8
}

pub fn recovery_ivl() -> Int {
  9
}

pub fn sndbuf() -> Int {
  11
}

pub fn rcvbuf() -> Int {
  12
}

pub fn rcvmore() -> Int {
  13
}

pub fn fd() -> Int {
  14
}

pub fn events() -> Int {
  15
}

pub fn type_option() -> Int {
  16
}

pub fn linger() -> Int {
  17
}

pub fn reconnect_ivl() -> Int {
  18
}

pub fn backlog() -> Int {
  19
}

pub fn reconnect_ivl_max() -> Int {
  21
}

pub fn maxmsgsize() -> Int {
  22
}

pub fn sndhwm() -> Int {
  23
}

pub fn rcvhwm() -> Int {
  24
}

pub fn multicast_hops() -> Int {
  25
}

pub fn rcvtimeo() -> Int {
  27
}

pub fn sndtimeo() -> Int {
  28
}

pub fn ipv4only() -> Int {
  31
}

pub fn last_endpoint() -> Int {
  32
}

pub fn router_mandatory() -> Int {
  33
}

pub fn tcp_keepalive() -> Int {
  34
}

pub fn tcp_keepalive_cnt() -> Int {
  35
}

pub fn tcp_keepalive_idle() -> Int {
  36
}

pub fn tcp_keepalive_intvl() -> Int {
  37
}

pub fn tcp_accept_filter() -> Int {
  38
}

pub fn immediate() -> Int {
  39
}

pub fn xpub_verbose() -> Int {
  40
}

pub fn ipv6() -> Int {
  42
}

pub fn mechanism() -> Int {
  43
}

pub fn plain_server() -> Int {
  44
}

pub fn plain_username() -> Int {
  45
}

pub fn plain_password() -> Int {
  46
}

pub fn curve_server() -> Int {
  47
}

pub fn curve_publickey() -> Int {
  48
}

pub fn curve_secretkey() -> Int {
  49
}

pub fn curve_serverkey() -> Int {
  50
}

pub fn probe_router() -> Int {
  51
}

pub fn req_correlate() -> Int {
  52
}

pub fn req_relaxed() -> Int {
  53
}

pub fn conflate() -> Int {
  54
}

pub fn zap_domain() -> Int {
  55
}

pub fn router_handover() -> Int {
  56
}

pub fn handshake_ivl() -> Int {
  66
}

pub fn heartbeat_ivl() -> Int {
  75
}

pub fn heartbeat_ttl() -> Int {
  76
}

pub fn heartbeat_timeout() -> Int {
  77
}

pub fn connect_timeout() -> Int {
  79
}

pub fn tcp_maxrt() -> Int {
  80
}

pub fn reconnect_stop() -> Int {
  109
}

pub fn omq_on_mute() -> Int {
  1004
}

pub fn omq_compression_level() -> Int {
  1005
}

pub fn omq_compression_dict() -> Int {
  1006
}

pub fn omq_compression_auto_train() -> Int {
  1007
}

pub fn omq_workload_profile() -> Int {
  1100
}

pub fn omq_on_mute_block() -> Int {
  0
}

pub fn omq_on_mute_drop_newest() -> Int {
  1
}

pub fn omq_on_mute_drop_oldest() -> Int {
  2
}
