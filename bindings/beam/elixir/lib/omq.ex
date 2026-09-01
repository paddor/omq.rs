defmodule OMQ do
  @moduledoc """
  Elixir API for OMQ.

  OMQ sockets are ZeroMQ-compatible message queues backed by the Rust
  `omq-tokio` runtime. Create a context with `context/0` or reuse the
  process-wide singleton with `context_instance/0`, then create sockets with
  `socket/2`.

  Endpoints are binaries or strings such as `"tcp://127.0.0.1:5555"`,
  `"ipc:///tmp/omq.sock"`, `"inproc://queue"`, `"lz4+tcp://127.0.0.1:5555"`,
  and `"zstd+tcp://127.0.0.1:5555"`.

  Functions return Erlang result shapes unchanged: `:ok`, `{:ok, value}`,
  `{:ok, value, metadata}` where documented by Erlang, or
  `{:error, class, reason}`.

  `send/3` accepts either an integer flags mask or an option list. Supported
  options are `:sndmore`, `:noblock`, `:dontwait`, `{:flags, flags}`, and
  `{:routing_id, id}`. `recv/1` and `recv/2` return either `{:ok, data}` or
  routing metadata maps for ROUTER/SERVER-style sockets.
  """

  @typedoc "Native OMQ context resource."
  @type context :: term()

  @typedoc "Native OMQ socket resource."
  @type socket :: term()

  @typedoc "Socket monitor event stream resource."
  @type monitor :: term()

  @typedoc "Common OMQ result shape."
  @type result(value) :: :ok | {:ok, value} | {:error, atom(), binary() | String.t()}

  @typedoc "Endpoint URI accepted by bind/connect."
  @type endpoint :: binary() | String.t()

  @typedoc "Send flags or option list."
  @type send_opts ::
          integer()
          | [
              :sndmore
              | :noblock
              | :dontwait
              | {:flags, integer()}
              | {:routing_id, non_neg_integer()}
            ]

  @doc "Create a context with one IO thread."
  def context, do: call(:context, [])

  @doc "Create a context with requested native IO thread count."
  def context(io_threads), do: call(:context, [io_threads])

  @doc "Return process-wide singleton context."
  def context_instance, do: call(:context_instance, [])

  @doc "Return process-wide singleton context, creating it with IO thread count."
  def context_instance(io_threads), do: call(:context_instance, [io_threads])

  @doc "Return process-wide singleton context."
  def instance, do: call(:instance, [])

  @doc "Return process-wide singleton context, creating it with IO thread count."
  def instance(io_threads), do: call(:instance, [io_threads])

  @doc "Terminate a context."
  def term(context), do: call(:term, [context])

  @doc "Terminate a context. Alias for `term/1`."
  def destroy(context), do: call(:destroy, [context])

  @doc "Return opaque native context share key."
  def context_share_key(context), do: call(:context_share_key, [context])

  @doc "Import native context by share key without owning its runtime."
  def context_from_share_key(share_key), do: call(:context_from_share_key, [share_key])

  @doc "Return whether this context wrapper or native core is closed."
  def context_closed(context), do: call(:context_closed, [context])

  @doc "Return native backend name."
  def backend_name, do: call(:backend_name, [])

  @doc "Return native binding version."
  def version, do: call(:version, [])

  @doc "Return native binding version. Alias for `version/0`."
  def omq_version, do: call(:omq_version, [])

  @doc "Return native binding version as `{major, minor, patch}`."
  def omq_version_info, do: call(:omq_version_info, [])

  @doc "Return libzmq compatibility version string."
  def zmq_version, do: call(:zmq_version, [])

  @doc "Return libzmq compatibility version tuple."
  def zmq_version_info, do: call(:zmq_version_info, [])

  @doc "Return POSIX strerror text for common libzmq errno values."
  def strerror(errno), do: call(:strerror, [errno])

  @doc "Return opaque native context share key."
  def share_key(context), do: call(:share_key, [context])

  @doc "Import native context by share key."
  def from_share_key(share_key), do: call(:from_share_key, [share_key])

  @doc "Create socket from context and socket type."
  def socket(context, type), do: call(:socket, [context, type])

  @doc "Bind socket to endpoint and return bound endpoint."
  def bind(socket, endpoint), do: call(:bind, [socket, endpoint])

  @doc "Bind socket to random ephemeral port on address."
  def bind_to_random_port(socket, addr), do: call(:bind_to_random_port, [socket, addr])

  @doc "Bind socket to random port in inclusive range."
  def bind_to_random_port(socket, addr, min_port, max_port),
    do: call(:bind_to_random_port, [socket, addr, min_port, max_port])

  @doc "Connect socket to endpoint."
  def connect(socket, endpoint), do: call(:connect, [socket, endpoint])

  @doc "Unbind socket from endpoint."
  def unbind(socket, endpoint), do: call(:unbind, [socket, endpoint])

  @doc "Disconnect socket from endpoint."
  def disconnect(socket, endpoint), do: call(:disconnect, [socket, endpoint])

  @doc "Create monitor stream for socket lifecycle events."
  def monitor(socket), do: call(:monitor, [socket])

  @doc "Receive next monitor event."
  def monitor_recv(monitor), do: call(:monitor_recv, [monitor])

  @doc "Receive next monitor event with timeout in milliseconds."
  def monitor_recv(monitor, timeout), do: call(:monitor_recv, [monitor, timeout])

  @doc "Try to receive one monitor event without blocking."
  def monitor_try_recv(monitor), do: call(:monitor_try_recv, [monitor])

  @doc "Return current connection snapshots for socket."
  def connections(socket), do: call(:connections, [socket])

  @doc "Return one connection snapshot by ID."
  def connection_info(socket, connection_id), do: call(:connection_info, [socket, connection_id])

  @doc "Send one binary message."
  def send(socket, data), do: call(:send, [socket, data])

  @doc "Send one binary message with flags or options."
  def send(socket, data, opts), do: call(:send, [socket, data, opts])

  @doc "Send one UTF-8 string message."
  def send_string(socket, text), do: call(:send_string, [socket, text])

  @doc "Send string with options or source encoding."
  def send_string(socket, text, opts_or_encoding),
    do: call(:send_string, [socket, text, opts_or_encoding])

  @doc "Send string using requested wire encoding and options."
  def send_string(socket, text, encoding, opts),
    do: call(:send_string, [socket, text, encoding, opts])

  @doc "Send one JSON value encoded by OTP `json`."
  def send_json(socket, value), do: call(:send_json, [socket, value])

  @doc "Send one JSON value with flags or options."
  def send_json(socket, value, opts), do: call(:send_json, [socket, value, opts])

  @doc "Send one Erlang term using external term format."
  def send_term(socket, term), do: call(:send_term, [socket, term])

  @doc "Send one Erlang term with flags or options."
  def send_term(socket, term, opts), do: call(:send_term, [socket, term, opts])

  @doc "Try to send one binary message without blocking."
  def try_send(socket, data), do: call(:try_send, [socket, data])

  @doc "Try to send one binary message with flags or options."
  def try_send(socket, data, opts), do: call(:try_send, [socket, data, opts])

  @doc "Send one multipart message."
  def send_multipart(socket, parts), do: call(:send_multipart, [socket, parts])

  @doc "Send one multipart message with flags or options."
  def send_multipart(socket, parts, opts), do: call(:send_multipart, [socket, parts, opts])

  @doc "Receive one message using socket timeout options."
  def recv(socket), do: call(:recv, [socket])

  @doc "Receive one message with timeout in milliseconds."
  def recv(socket, timeout), do: call(:recv, [socket, timeout])

  @doc "Receive one UTF-8 string message."
  def recv_string(socket), do: call(:recv_string, [socket])

  @doc "Receive one string with timeout or wire encoding."
  def recv_string(socket, timeout_or_encoding),
    do: call(:recv_string, [socket, timeout_or_encoding])

  @doc "Receive one string with timeout and wire encoding."
  def recv_string(socket, timeout, encoding), do: call(:recv_string, [socket, timeout, encoding])

  @doc "Receive one JSON value decoded by OTP `json`."
  def recv_json(socket), do: call(:recv_json, [socket])

  @doc "Receive one JSON value with timeout in milliseconds."
  def recv_json(socket, timeout), do: call(:recv_json, [socket, timeout])

  @doc "Try to receive one JSON value without blocking."
  def try_recv_json(socket), do: call(:try_recv_json, [socket])

  @doc "Receive one Erlang term encoded by `send_term/2,3`."
  def recv_term(socket), do: call(:recv_term, [socket])

  @doc "Receive one Erlang term with timeout in milliseconds."
  def recv_term(socket, timeout), do: call(:recv_term, [socket, timeout])

  @doc "Try to receive one Erlang term without blocking."
  def try_recv_term(socket), do: call(:try_recv_term, [socket])

  @doc "Receive next frame from multipart message."
  def recv_frame(socket), do: call(:recv_frame, [socket])

  @doc "Receive next frame with timeout in milliseconds."
  def recv_frame(socket, timeout), do: call(:recv_frame, [socket, timeout])

  @doc "Try to receive one message without blocking."
  def try_recv(socket), do: call(:try_recv, [socket])

  @doc "Try to receive one UTF-8 string without blocking."
  def try_recv_string(socket), do: call(:try_recv_string, [socket])

  @doc "Try to receive one string with wire encoding."
  def try_recv_string(socket, encoding), do: call(:try_recv_string, [socket, encoding])

  @doc "Receive one multipart message using socket timeout options."
  def recv_multipart(socket), do: call(:recv_multipart, [socket])

  @doc "Receive one multipart message with timeout in milliseconds."
  def recv_multipart(socket, timeout), do: call(:recv_multipart, [socket, timeout])

  @doc "Try to receive one multipart message without blocking."
  def try_recv_multipart(socket), do: call(:try_recv_multipart, [socket])

  @doc "Poll socket readiness entries with timeout in milliseconds."
  def poll(entries, timeout), do: call(:poll, [entries, timeout])

  @doc "Return ready read, write, and exception socket lists."
  def select(rlist, wlist, xlist, timeout), do: call(:select, [rlist, wlist, xlist, timeout])

  @doc "Run bidirectional proxy between two sockets."
  def proxy(frontend, backend), do: call(:proxy, [frontend, backend])

  @doc "Run bidirectional proxy and mirror traffic to capture socket."
  def proxy(frontend, backend, capture), do: call(:proxy, [frontend, backend, capture])

  @doc "Run steerable proxy with PAUSE, RESUME, and TERMINATE control."
  def proxy_steerable(frontend, backend, capture, control),
    do: call(:proxy_steerable, [frontend, backend, capture, control])

  @doc "Run libzmq-compatible device. Device type is accepted for parity."
  def device(device_type, frontend, backend), do: call(:device, [device_type, frontend, backend])

  @doc "Return whether native feature or transport is available."
  def has(capability), do: call(:has, [capability])

  @doc "Generate CURVE public/secret keypair."
  def curve_keypair, do: call(:curve_keypair, [])

  @doc "Derive CURVE public key from secret key."
  def curve_public(secret), do: call(:curve_public, [secret])

  @doc "Subscribe SUB or XSUB socket to prefix."
  def subscribe(socket, prefix), do: call(:subscribe, [socket, prefix])

  @doc "Remove SUB or XSUB prefix subscription."
  def unsubscribe(socket, prefix), do: call(:unsubscribe, [socket, prefix])

  @doc "Join RADIO/DISH group."
  def join(socket, group), do: call(:join, [socket, group])

  @doc "Leave RADIO/DISH group."
  def leave(socket, group), do: call(:leave, [socket, group])

  @doc "Send RADIO message to group."
  def send_group(socket, group, body), do: call(:send_group, [socket, group, body])

  @doc "Close socket with zero linger."
  def close(socket), do: call(:close, [socket])

  @doc "Close socket with explicit linger in milliseconds."
  def close(socket, linger), do: call(:close, [socket, linger])

  @doc "Wait until at least `min_peers` peers are connected."
  def wait_connected(socket, min_peers, timeout),
    do: call(:wait_connected, [socket, min_peers, timeout])

  @doc "Wait until at least `min_subscriptions` subscriptions are visible."
  def wait_subscribed(socket, min_subscriptions, timeout),
    do: call(:wait_subscribed, [socket, min_subscriptions, timeout])

  @doc "Set socket option. Alias for `setsockopt/3`."
  def set(socket, option, value), do: call(:set, [socket, option, value])

  @doc "Get socket option. Alias for `getsockopt/2`."
  def get(socket, option), do: call(:get, [socket, option])

  @doc "Set socket option by atom or integer option ID."
  def setsockopt(socket, option, value), do: call(:setsockopt, [socket, option, value])

  @doc "Get socket option by atom or integer option ID."
  def getsockopt(socket, option), do: call(:getsockopt, [socket, option])

  @doc "Set both SNDHWM and RCVHWM."
  def set_hwm(socket, value), do: call(:set_hwm, [socket, value])

  @doc "Return SNDHWM as compatibility HWM value."
  def get_hwm(socket), do: call(:get_hwm, [socket])

  @doc "Set binary socket option from UTF-8 text."
  def setsockopt_string(socket, option, text),
    do: call(:setsockopt_string, [socket, option, text])

  @doc "Get binary socket option as UTF-8 text."
  def getsockopt_string(socket, option), do: call(:getsockopt_string, [socket, option])

  @doc "Return socket type atom."
  def socket_type(socket), do: call(:socket_type, [socket])

  @doc "Return wrapper socket ID."
  def socket_id(socket), do: call(:socket_id, [socket])

  @doc "Return whether socket wrapper is closed."
  def closed(socket), do: call(:closed, [socket])

  @doc "PAIR socket type constant."
  def pair, do: call(:pair, [])

  @doc "PUB socket type constant."
  def pub, do: call(:pub, [])

  @doc "SUB socket type constant."
  def sub, do: call(:sub, [])

  @doc "REQ socket type constant."
  def req, do: call(:req, [])

  @doc "REP socket type constant."
  def rep, do: call(:rep, [])

  @doc "DEALER socket type constant."
  def dealer, do: call(:dealer, [])

  @doc "ROUTER socket type constant."
  def router, do: call(:router, [])

  @doc "PULL socket type constant."
  def pull, do: call(:pull, [])

  @doc "PUSH socket type constant."
  def push, do: call(:push, [])

  @doc "XPUB socket type constant."
  def xpub, do: call(:xpub, [])

  @doc "XSUB socket type constant."
  def xsub, do: call(:xsub, [])

  @doc "STREAM socket type constant."
  def stream, do: call(:stream, [])

  @doc "SERVER socket type constant."
  def server, do: call(:server, [])

  @doc "CLIENT socket type constant."
  def client, do: call(:client, [])

  @doc "RADIO socket type constant."
  def radio, do: call(:radio, [])

  @doc "DISH socket type constant."
  def dish, do: call(:dish, [])

  @doc "GATHER socket type constant."
  def gather, do: call(:gather, [])

  @doc "SCATTER socket type constant."
  def scatter, do: call(:scatter, [])

  @doc "PEER socket type constant."
  def peer, do: call(:peer, [])

  @doc "CHANNEL socket type constant."
  def channel, do: call(:channel, [])

  @doc "POLLIN readiness flag."
  def pollin, do: call(:pollin, [])

  @doc "POLLOUT readiness flag."
  def pollout, do: call(:pollout, [])

  @doc "POLLERR readiness flag."
  def pollerr, do: call(:pollerr, [])

  @doc "POLLPRI readiness flag."
  def pollpri, do: call(:pollpri, [])

  @doc "SNDMORE send flag."
  def sndmore, do: call(:sndmore, [])

  @doc "NOBLOCK send/receive flag."
  def noblock, do: call(:noblock, [])

  @doc "DONTWAIT alias for NOBLOCK."
  def dontwait, do: call(:dontwait, [])

  @doc "HWM compatibility option ID."
  def hwm, do: call(:hwm, [])

  @doc "AFFINITY option ID."
  def affinity, do: call(:affinity, [])

  @doc "IDENTITY option ID."
  def identity, do: call(:identity, [])

  @doc "ROUTING_ID option ID."
  def routing_id, do: call(:routing_id, [])

  @doc "SUBSCRIBE option ID."
  def subscribe_opt, do: call(:subscribe_opt, [])

  @doc "UNSUBSCRIBE option ID."
  def unsubscribe_opt, do: call(:unsubscribe_opt, [])

  @doc "RCVMORE option ID."
  def rcvmore, do: call(:rcvmore, [])

  @doc "FD option ID."
  def fd, do: call(:fd, [])

  @doc "EVENTS option ID."
  def events, do: call(:events, [])

  @doc "TYPE option ID."
  def type, do: call(:type, [])

  @doc "BACKLOG option ID."
  def backlog, do: call(:backlog, [])

  @doc "LINGER option ID."
  def linger, do: call(:linger, [])

  @doc "RECONNECT_IVL option ID."
  def reconnect_ivl, do: call(:reconnect_ivl, [])

  @doc "RECONNECT_IVL_MAX option ID."
  def reconnect_ivl_max, do: call(:reconnect_ivl_max, [])

  @doc "MAXMSGSIZE option ID."
  def maxmsgsize, do: call(:maxmsgsize, [])

  @doc "SNDHWM option ID."
  def sndhwm, do: call(:sndhwm, [])

  @doc "RCVHWM option ID."
  def rcvhwm, do: call(:rcvhwm, [])

  @doc "RCVTIMEO option ID."
  def rcvtimeo, do: call(:rcvtimeo, [])

  @doc "SNDTIMEO option ID."
  def sndtimeo, do: call(:sndtimeo, [])

  @doc "ROUTER_MANDATORY option ID."
  def router_mandatory, do: call(:router_mandatory, [])

  @doc "TCP_KEEPALIVE option ID."
  def tcp_keepalive, do: call(:tcp_keepalive, [])

  @doc "TCP_KEEPALIVE_CNT option ID."
  def tcp_keepalive_cnt, do: call(:tcp_keepalive_cnt, [])

  @doc "TCP_KEEPALIVE_IDLE option ID."
  def tcp_keepalive_idle, do: call(:tcp_keepalive_idle, [])

  @doc "TCP_KEEPALIVE_INTVL option ID."
  def tcp_keepalive_intvl, do: call(:tcp_keepalive_intvl, [])

  @doc "SNDBUF option ID."
  def sndbuf, do: call(:sndbuf, [])

  @doc "RCVBUF option ID."
  def rcvbuf, do: call(:rcvbuf, [])

  @doc "CONFLATE option ID."
  def conflate, do: call(:conflate, [])

  @doc "HANDSHAKE_IVL option ID."
  def handshake_ivl, do: call(:handshake_ivl, [])

  @doc "HEARTBEAT_IVL option ID."
  def heartbeat_ivl, do: call(:heartbeat_ivl, [])

  @doc "HEARTBEAT_TTL option ID."
  def heartbeat_ttl, do: call(:heartbeat_ttl, [])

  @doc "HEARTBEAT_TIMEOUT option ID."
  def heartbeat_timeout, do: call(:heartbeat_timeout, [])

  @doc "RECONNECT_STOP option ID."
  def reconnect_stop, do: call(:reconnect_stop, [])

  @doc "IMMEDIATE option ID."
  def immediate, do: call(:immediate, [])

  @doc "IPV6 option ID."
  def ipv6, do: call(:ipv6, [])

  @doc "IPV4ONLY option ID."
  def ipv4only, do: call(:ipv4only, [])

  @doc "RATE option ID."
  def rate, do: call(:rate, [])

  @doc "CONNECT_TIMEOUT option ID."
  def connect_timeout, do: call(:connect_timeout, [])

  @doc "XPUB_VERBOSE option ID."
  def xpub_verbose, do: call(:xpub_verbose, [])

  @doc "PROBE_ROUTER option ID."
  def probe_router, do: call(:probe_router, [])

  @doc "REQ_CORRELATE option ID."
  def req_correlate, do: call(:req_correlate, [])

  @doc "REQ_RELAXED option ID."
  def req_relaxed, do: call(:req_relaxed, [])

  @doc "ROUTER_HANDOVER option ID."
  def router_handover, do: call(:router_handover, [])

  @doc "TCP_ACCEPT_FILTER option ID."
  def tcp_accept_filter, do: call(:tcp_accept_filter, [])

  @doc "TCP_MAXRT option ID."
  def tcp_maxrt, do: call(:tcp_maxrt, [])

  @doc "MULTICAST_HOPS option ID."
  def multicast_hops, do: call(:multicast_hops, [])

  @doc "RECOVERY_IVL option ID."
  def recovery_ivl, do: call(:recovery_ivl, [])

  @doc "ZAP_DOMAIN option ID."
  def zap_domain, do: call(:zap_domain, [])

  @doc "MECHANISM option ID."
  def mechanism, do: call(:mechanism, [])

  @doc "PLAIN_SERVER option ID."
  def plain_server, do: call(:plain_server, [])

  @doc "PLAIN_USERNAME option ID."
  def plain_username, do: call(:plain_username, [])

  @doc "PLAIN_PASSWORD option ID."
  def plain_password, do: call(:plain_password, [])

  @doc "CURVE_SERVER option ID."
  def curve_server, do: call(:curve_server, [])

  @doc "CURVE_PUBLICKEY option ID."
  def curve_publickey, do: call(:curve_publickey, [])

  @doc "CURVE_SECRETKEY option ID."
  def curve_secretkey, do: call(:curve_secretkey, [])

  @doc "CURVE_SERVERKEY option ID."
  def curve_serverkey, do: call(:curve_serverkey, [])

  @doc "LAST_ENDPOINT option ID."
  def last_endpoint, do: call(:last_endpoint, [])

  @doc "OMQ_ON_MUTE option ID."
  def omq_on_mute, do: call(:omq_on_mute, [])

  @doc "OMQ_COMPRESSION_LEVEL option ID."
  def omq_compression_level, do: call(:omq_compression_level, [])

  @doc "OMQ_COMPRESSION_DICT option ID."
  def omq_compression_dict, do: call(:omq_compression_dict, [])

  @doc "OMQ_COMPRESSION_AUTO_TRAIN option ID."
  def omq_compression_auto_train, do: call(:omq_compression_auto_train, [])

  @doc "OMQ_WORKLOAD_PROFILE option ID."
  def omq_workload_profile, do: call(:omq_workload_profile, [])

  @doc "OMQ_ON_MUTE block mode value."
  def omq_on_mute_block, do: call(:omq_on_mute_block, [])

  @doc "OMQ_ON_MUTE drop-newest mode value."
  def omq_on_mute_drop_newest, do: call(:omq_on_mute_drop_newest, [])

  @doc "OMQ_ON_MUTE drop-oldest mode value."
  def omq_on_mute_drop_oldest, do: call(:omq_on_mute_drop_oldest, [])

  @doc "FORWARDER device type constant."
  def forwarder, do: call(:forwarder, [])

  @doc "QUEUE device type constant."
  def queue, do: call(:queue, [])

  @doc "STREAMER device type constant."
  def streamer, do: call(:streamer, [])

  @doc "NULL mechanism constant."
  def null, do: call(:null, [])

  @doc "PLAIN mechanism constant."
  def plain, do: call(:plain, [])

  @doc "CURVE mechanism constant."
  def curve, do: call(:curve, [])

  defp call(function, args), do: :erlang.apply(:omq, function, args)
end
