import 'dart:typed_data';

/// OMQ socket types. DGRAM is not supported.
enum SocketType {
  /// Exclusive pair.
  pair(0),

  /// Publisher.
  pub(1),

  /// Subscriber.
  sub(2),

  /// Request client.
  req(3),

  /// Reply server.
  rep(4),

  /// Asynchronous request client.
  dealer(5),

  /// Identity-routing server.
  router(6),

  /// Fair-queued receiver.
  pull(7),

  /// Round-robin sender.
  push(8),

  /// Publisher that receives subscriptions.
  xpub(9),

  /// Subscriber that sends subscriptions.
  xsub(10),

  /// Raw TCP.
  stream(11),

  /// Single-part ROUTER.
  server(12),

  /// Single-part DEALER.
  client(13),

  /// Group publisher.
  radio(14),

  /// Group subscriber.
  dish(15),

  /// Single-part PULL.
  gather(16),

  /// Single-part PUSH.
  scatter(17),

  /// N:N identity-routed peer.
  peer(19),

  /// Single-part PAIR.
  channel(20);

  const SocketType(this.value);

  /// The native `ZMQ_*` socket type value.
  final int value;
}

/// Native context option numbers (`ZMQ_*` for `zmq_ctx_set`).
abstract final class ContextOption {
  /// `ZMQ_IO_THREADS`.
  static const int ioThreads = 1;

  /// `ZMQ_MAX_SOCKETS`.
  static const int maxSockets = 2;

  /// `ZMQ_SOCKET_LIMIT`.
  static const int socketLimit = 3;

  /// `ZMQ_MAX_MSGSZ`.
  static const int maxMessageSize = 5;

  /// `ZMQ_BLOCKY`.
  static const int blocky = 70;
}

/// Native socket option numbers (`ZMQ_*` and `OMQ_*` for `zmq_setsockopt`).
abstract final class SocketOption {
  /// `ZMQ_AFFINITY` (int64).
  static const int affinity = 4;

  /// `ZMQ_ROUTING_ID` (bytes).
  static const int routingId = 5;

  /// `ZMQ_SUBSCRIBE` (bytes).
  static const int subscribe = 6;

  /// `ZMQ_UNSUBSCRIBE` (bytes).
  static const int unsubscribe = 7;

  /// `ZMQ_SNDBUF`.
  static const int sendBuffer = 11;

  /// `ZMQ_RCVBUF`.
  static const int receiveBuffer = 12;

  /// `ZMQ_RCVMORE`.
  static const int receiveMore = 13;

  /// `ZMQ_FD`.
  static const int fileDescriptor = 14;

  /// `ZMQ_EVENTS`.
  static const int events = 15;

  /// `ZMQ_TYPE`.
  static const int type = 16;

  /// `ZMQ_LINGER` (ms).
  static const int linger = 17;

  /// `ZMQ_RECONNECT_IVL` (ms).
  static const int reconnectInterval = 18;

  /// `ZMQ_BACKLOG`.
  static const int backlog = 19;

  /// `ZMQ_RECONNECT_IVL_MAX` (ms).
  static const int reconnectIntervalMax = 21;

  /// `ZMQ_MAXMSGSIZE` (int64).
  static const int maxMessageSize = 22;

  /// `ZMQ_SNDHWM`.
  static const int sendHwm = 23;

  /// `ZMQ_RCVHWM`.
  static const int receiveHwm = 24;

  /// `ZMQ_RCVTIMEO` (ms).
  static const int receiveTimeout = 27;

  /// `ZMQ_SNDTIMEO` (ms).
  static const int sendTimeout = 28;

  /// `ZMQ_LAST_ENDPOINT` (string).
  static const int lastEndpoint = 32;

  /// `ZMQ_ROUTER_MANDATORY`.
  static const int routerMandatory = 33;

  /// `ZMQ_TCP_KEEPALIVE`.
  static const int tcpKeepalive = 34;

  /// `ZMQ_TCP_KEEPALIVE_CNT`.
  static const int tcpKeepaliveCount = 35;

  /// `ZMQ_TCP_KEEPALIVE_IDLE`.
  static const int tcpKeepaliveIdle = 36;

  /// `ZMQ_TCP_KEEPALIVE_INTVL`.
  static const int tcpKeepaliveInterval = 37;

  /// `ZMQ_IMMEDIATE`.
  static const int immediate = 39;

  /// `ZMQ_XPUB_VERBOSE`.
  static const int xpubVerbose = 40;

  /// `ZMQ_IPV6`.
  static const int ipv6 = 42;

  /// `ZMQ_MECHANISM`.
  static const int mechanism = 43;

  /// `ZMQ_PLAIN_SERVER`.
  static const int plainServer = 44;

  /// `ZMQ_PLAIN_USERNAME` (string).
  static const int plainUsername = 45;

  /// `ZMQ_PLAIN_PASSWORD` (string).
  static const int plainPassword = 46;

  /// `ZMQ_CURVE_SERVER`.
  static const int curveServer = 47;

  /// `ZMQ_CURVE_PUBLICKEY` (Z85 string).
  static const int curvePublicKey = 48;

  /// `ZMQ_CURVE_SECRETKEY` (Z85 string).
  static const int curveSecretKey = 49;

  /// `ZMQ_CURVE_SERVERKEY` (Z85 string).
  static const int curveServerKey = 50;

  /// `ZMQ_PROBE_ROUTER`.
  static const int probeRouter = 51;

  /// `ZMQ_REQ_CORRELATE`.
  static const int reqCorrelate = 52;

  /// `ZMQ_REQ_RELAXED`.
  static const int reqRelaxed = 53;

  /// `ZMQ_CONFLATE`.
  static const int conflate = 54;

  /// `ZMQ_ZAP_DOMAIN` (string).
  static const int zapDomain = 55;

  /// `ZMQ_ROUTER_HANDOVER`.
  static const int routerHandover = 56;

  /// `ZMQ_CONNECT_ROUTING_ID` (bytes).
  static const int connectRoutingId = 61;

  /// `ZMQ_HANDSHAKE_IVL` (ms).
  static const int handshakeInterval = 66;

  /// `ZMQ_XPUB_NODROP`.
  static const int xpubNoDrop = 69;

  /// `ZMQ_XPUB_MANUAL`.
  static const int xpubManual = 71;

  /// `ZMQ_XPUB_WELCOME_MSG` (bytes).
  static const int xpubWelcomeMessage = 72;

  /// `ZMQ_INVERT_MATCHING`.
  static const int invertMatching = 74;

  /// `ZMQ_HEARTBEAT_IVL` (ms).
  static const int heartbeatInterval = 75;

  /// `ZMQ_HEARTBEAT_TTL` (ms).
  static const int heartbeatTtl = 76;

  /// `ZMQ_HEARTBEAT_TIMEOUT` (ms).
  static const int heartbeatTimeout = 77;

  /// `ZMQ_XPUB_VERBOSER`.
  static const int xpubVerboser = 78;

  /// `ZMQ_CONNECT_TIMEOUT` (ms).
  static const int connectTimeout = 79;

  /// `ZMQ_ZAP_ENFORCE_DOMAIN`.
  static const int zapEnforceDomain = 93;

  /// `ZMQ_ROUTER_NOTIFY`.
  static const int routerNotify = 97;

  /// `ZMQ_RECONNECT_STOP`.
  static const int reconnectStop = 109;

  /// `ZMQ_HELLO_MSG` (bytes).
  static const int helloMessage = 110;

  /// `ZMQ_DISCONNECT_MSG` (bytes).
  static const int disconnectMessage = 111;

  /// `ZMQ_HICCUP_MSG` (bytes).
  static const int hiccupMessage = 114;

  /// `OMQ_ARENA_THRESHOLD` (int64 bytes).
  static const int arenaThreshold = 10001;
}

/// Options applied when a socket is created. Null fields keep the native
/// default.
final class SocketOptions {
  /// Creates socket options.
  const SocketOptions({
    this.linger,
    this.sendHwm,
    this.receiveHwm,
    this.sendTimeout,
    this.receiveTimeout,
    this.routingId,
    this.connectRoutingId,
    this.reconnectInterval,
    this.reconnectIntervalMax,
    this.handshakeInterval,
    this.connectTimeout,
    this.heartbeatInterval,
    this.heartbeatTtl,
    this.heartbeatTimeout,
    this.maxMessageSize,
    this.backlog,
    this.immediate,
    this.ipv6,
    this.conflate,
    this.routerMandatory,
    this.routerHandover,
    this.probeRouter,
    this.reqCorrelate,
    this.reqRelaxed,
    this.xpubVerbose,
    this.xpubNoDrop,
    this.xpubManual,
    this.invertMatching,
    this.zapDomain,
  });

  /// How long close waits to deliver queued messages. Native default:
  /// forever.
  final Duration? linger;

  /// Send high-water mark, in messages.
  final int? sendHwm;

  /// Receive high-water mark, in messages.
  final int? receiveHwm;

  /// Timeout for blocking sync sends.
  final Duration? sendTimeout;

  /// Timeout for blocking sync receives.
  final Duration? receiveTimeout;

  /// This socket's routing ID.
  final Uint8List? routingId;

  /// The routing ID assigned to the peer of the next connect.
  final Uint8List? connectRoutingId;

  /// Initial reconnect delay.
  final Duration? reconnectInterval;

  /// Maximum reconnect delay.
  final Duration? reconnectIntervalMax;

  /// Handshake timeout.
  final Duration? handshakeInterval;

  /// TCP connect timeout.
  final Duration? connectTimeout;

  /// Interval between ZMTP heartbeats.
  final Duration? heartbeatInterval;

  /// Heartbeat TTL announced to peers.
  final Duration? heartbeatTtl;

  /// How long to wait for a heartbeat reply.
  final Duration? heartbeatTimeout;

  /// Largest accepted inbound message, in bytes.
  final int? maxMessageSize;

  /// Listen backlog.
  final int? backlog;

  /// Queue only on completed connections.
  final bool? immediate;

  /// Enable IPv6.
  final bool? ipv6;

  /// Keep only the last message.
  final bool? conflate;

  /// ROUTER fails sends to unknown peers.
  final bool? routerMandatory;

  /// ROUTER hands an identity over to a new peer.
  final bool? routerHandover;

  /// Send an empty message to new ROUTER peers.
  final bool? probeRouter;

  /// REQ matches replies to requests.
  final bool? reqCorrelate;

  /// REQ allows a new request before a reply.
  final bool? reqRelaxed;

  /// XPUB passes duplicate subscriptions.
  final bool? xpubVerbose;

  /// XPUB blocks instead of dropping for mute peers.
  final bool? xpubNoDrop;

  /// XPUB subscriptions are managed by the application.
  final bool? xpubManual;

  /// SUB receives messages that do not match.
  final bool? invertMatching;

  /// ZAP domain.
  final String? zapDomain;
}
