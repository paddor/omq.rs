module OMQ
  VERSION = "0.1.0"

  PAIR    =  0
  PUB     =  1
  SUB     =  2
  REQ     =  3
  REP     =  4
  DEALER  =  5
  ROUTER  =  6
  PULL    =  7
  PUSH    =  8
  XPUB    =  9
  XSUB    = 10
  STREAM  = 11
  SERVER  = 12
  CLIENT  = 13
  RADIO   = 14
  DISH    = 15
  GATHER  = 16
  SCATTER = 17
  DGRAM   = 18
  PEER    = 19
  CHANNEL = 20

  DONTWAIT = 1
  NOBLOCK  = DONTWAIT
  SNDMORE  = 2

  POLLIN  = 1
  POLLOUT = 2
  POLLERR = 4
  POLLPRI = 8

  AFFINITY                          =  4
  ROUTING_ID                        =  5
  IDENTITY                          =  5
  SUBSCRIBE                         =  6
  UNSUBSCRIBE                       =  7
  RATE                              =  8
  RECOVERY_IVL                      =  9
  SNDBUF                            = 11
  RCVBUF                            = 12
  RCVMORE                           = 13
  FD                                = 14
  EVENTS                            = 15
  TYPE                              = 16
  LINGER                            = 17
  RECONNECT_IVL                     = 18
  BACKLOG                           = 19
  RECONNECT_IVL_MAX                 = 21
  MAXMSGSIZE                        = 22
  SNDHWM                            = 23
  RCVHWM                            = 24
  MULTICAST_HOPS                    = 25
  RCVTIMEO                          = 27
  SNDTIMEO                          = 28
  IPV4ONLY                          = 31
  LAST_ENDPOINT                     = 32
  ROUTER_MANDATORY                  = 33
  TCP_KEEPALIVE                     = 34
  TCP_KEEPALIVE_CNT                 = 35
  TCP_KEEPALIVE_IDLE                = 36
  TCP_KEEPALIVE_INTVL               = 37
  TCP_ACCEPT_FILTER                 = 38
  IMMEDIATE                         = 39
  XPUB_VERBOSE                      = 40
  ROUTER_RAW                        = 41
  IPV6                              = 42
  MECHANISM                         = 43
  PLAIN_SERVER                      = 44
  PLAIN_USERNAME                    = 45
  PLAIN_PASSWORD                    = 46
  CURVE_SERVER                      = 47
  CURVE_PUBLICKEY                   = 48
  CURVE_SECRETKEY                   = 49
  CURVE_SERVERKEY                   = 50
  PROBE_ROUTER                      = 51
  REQ_CORRELATE                     = 52
  REQ_RELAXED                       = 53
  CONFLATE                          = 54
  ZAP_DOMAIN                        = 55
  ROUTER_HANDOVER                   = 56
  TOS                               = 57
  IPC_FILTER_PID                    = 58
  IPC_FILTER_UID                    = 59
  IPC_FILTER_GID                    = 60
  CONNECT_ROUTING_ID                = 61
  CONNECT_RID                       = CONNECT_ROUTING_ID
  GSSAPI_SERVER                     = 62
  GSSAPI_PRINCIPAL                  = 63
  GSSAPI_SERVICE_PRINCIPAL          = 64
  GSSAPI_PLAINTEXT                  = 65
  HANDSHAKE_IVL                     = 66
  SOCKS_PROXY                       = 68
  XPUB_NODROP                       = 69
  BLOCKY                            = 70
  XPUB_MANUAL                       = 71
  XPUB_WELCOME_MSG                  = 72
  STREAM_NOTIFY                     = 73
  INVERT_MATCHING                   = 74
  HEARTBEAT_IVL                     = 75
  HEARTBEAT_TTL                     = 76
  HEARTBEAT_TIMEOUT                 = 77
  XPUB_VERBOSER                     = 78
  XPUB_VERBOSE_UNSUBSCRIBE          = XPUB_VERBOSER
  CONNECT_TIMEOUT                   =  79
  TCP_MAXRT                         =  80
  THREAD_SAFE                       =  81
  MULTICAST_MAXTPDU                 =  84
  VMCI_BUFFER_SIZE                  =  85
  VMCI_BUFFER_MIN_SIZE              =  86
  VMCI_BUFFER_MAX_SIZE              =  87
  VMCI_CONNECT_TIMEOUT              =  88
  USE_FD                            =  89
  GSSAPI_PRINCIPAL_NAMETYPE         =  90
  GSSAPI_SERVICE_PRINCIPAL_NAMETYPE =  91
  BINDTODEVICE                      =  92
  ZAP_ENFORCE_DOMAIN                =  93
  LOOPBACK_FASTPATH                 =  94
  METADATA                          =  95
  MULTICAST_LOOP                    =  96
  ROUTER_NOTIFY                     =  97
  XPUB_MANUAL_LAST_VALUE            =  98
  SOCKS_USERNAME                    =  99
  SOCKS_PASSWORD                    = 100
  IN_BATCH_SIZE                     = 101
  OUT_BATCH_SIZE                    = 102
  WSS_KEY_PEM                       = 103
  WSS_CERT_PEM                      = 104
  WSS_TRUST_PEM                     = 105
  WSS_HOSTNAME                      = 106
  WSS_TRUST_SYSTEM                  = 107
  ONLY_FIRST_SUBSCRIBE              = 108
  RECONNECT_STOP                    = 109
  HELLO_MSG                         = 110
  DISCONNECT_MSG                    = 111
  PRIORITY                          = 112
  BUSY_POLL                         = 113
  HICCUP_MSG                        = 114
  XSUB_VERBOSE_UNSUBSCRIBE          = 115
  TOPICS_COUNT                      = 116
  NORM_MODE                         = 117
  NORM_UNICAST_NACK                 = 118
  NORM_BUFFER_SIZE                  = 119
  NORM_SEGMENT_SIZE                 = 120
  NORM_BLOCK_SIZE                   = 121
  NORM_NUM_PARITY                   = 122
  NORM_NUM_AUTOPARITY               = 123
  NORM_PUSH                         = 124

  DELAY_ATTACH_ON_CONNECT = IMMEDIATE
  FAIL_UNROUTABLE         = ROUTER_MANDATORY
  ROUTER_BEHAVIOR         = ROUTER_MANDATORY

  SRCFD  = 2
  MORE   = 1
  SHARED = 3

  NULL_MECHANISM   = 0
  PLAIN_MECHANISM  = 1
  CURVE_MECHANISM  = 2
  GSSAPI_MECHANISM = 3

  GSSAPI_NT_HOSTBASED      = 0
  GSSAPI_NT_USER_NAME      = 1
  GSSAPI_NT_KRB5_PRINCIPAL = 2

  NORM_FIXED                      =   0
  NORM_CC                         =   1
  NORM_CCL                        =   2
  NORM_CCE                        =   3
  NORM_CCE_ECNONLY                =   4
  RECONNECT_STOP_CONN_REFUSED     = 0x1
  RECONNECT_STOP_HANDSHAKE_FAILED = 0x2
  RECONNECT_STOP_AFTER_DISCONNECT = 0x4

  IO_THREADS                 =  1
  MAX_SOCKETS                =  2
  SOCKET_LIMIT               =  3
  THREAD_PRIORITY            =  3
  THREAD_SCHED_POLICY        =  4
  MAX_MSGSZ                  =  5
  MSG_T_SIZE                 =  6
  THREAD_AFFINITY_CPU_ADD    =  7
  THREAD_AFFINITY_CPU_REMOVE =  8
  THREAD_NAME_PREFIX         =  9
  ZERO_COPY_RECV             = 10
  IPV6_CTX                   = 42

  MAX_SOCKETS_DFLT         = 1023
  IO_THREADS_DFLT          =    1
  THREAD_PRIORITY_DFLT     =   -1
  THREAD_SCHED_POLICY_DFLT =   -1

  GROUP_MAX_LENGTH = 255

  EVENT_CONNECTED                  =       1
  EVENT_CONNECT_DELAYED            =       2
  EVENT_CONNECT_RETRIED            =       4
  EVENT_LISTENING                  =       8
  EVENT_BIND_FAILED                =      16
  EVENT_ACCEPTED                   =      32
  EVENT_ACCEPT_FAILED              =      64
  EVENT_CLOSED                     =     128
  EVENT_CLOSE_FAILED               =     256
  EVENT_DISCONNECTED               =     512
  EVENT_MONITOR_STOPPED            =    1024
  EVENT_HANDSHAKE_FAILED_NO_DETAIL =    2048
  EVENT_HANDSHAKE_SUCCEEDED        =    4096
  EVENT_HANDSHAKE_FAILED_PROTOCOL  =    8192
  EVENT_HANDSHAKE_FAILED_AUTH      =   16384
  EVENT_ALL                        =  0xFFFF
  EVENT_PIPES_STATS                = 0x10000
  CURRENT_EVENT_VERSION            =       1
  CURRENT_EVENT_VERSION_DRAFT      =       2
  EVENT_ALL_V1                     = EVENT_ALL
  EVENT_ALL_V2                     = EVENT_ALL_V1 | EVENT_PIPES_STATS

  PROTOCOL_ERROR_ZMTP_UNSPECIFIED                   = 0x10000000
  PROTOCOL_ERROR_ZMTP_UNEXPECTED_COMMAND            = 0x10000001
  PROTOCOL_ERROR_ZMTP_INVALID_SEQUENCE              = 0x10000002
  PROTOCOL_ERROR_ZMTP_KEY_EXCHANGE                  = 0x10000003
  PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_UNSPECIFIED = 0x10000011
  PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_MESSAGE     = 0x10000012
  PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_HELLO       = 0x10000013
  PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_INITIATE    = 0x10000014
  PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_ERROR       = 0x10000015
  PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_READY       = 0x10000016
  PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_WELCOME     = 0x10000017
  PROTOCOL_ERROR_ZMTP_INVALID_METADATA              = 0x10000018
  PROTOCOL_ERROR_ZMTP_CRYPTOGRAPHIC                 = 0x11000001
  PROTOCOL_ERROR_ZMTP_MECHANISM_MISMATCH            = 0x11000002
  PROTOCOL_ERROR_ZAP_UNSPECIFIED                    = 0x20000000
  PROTOCOL_ERROR_ZAP_MALFORMED_REPLY                = 0x20000001
  PROTOCOL_ERROR_ZAP_BAD_REQUEST_ID                 = 0x20000002
  PROTOCOL_ERROR_ZAP_BAD_VERSION                    = 0x20000003
  PROTOCOL_ERROR_ZAP_INVALID_STATUS_CODE            = 0x20000004
  PROTOCOL_ERROR_ZAP_INVALID_METADATA               = 0x20000005
  PROTOCOL_ERROR_WS_UNSPECIFIED                     = 0x30000000

  MSG_PROPERTY_ROUTING_ID   = "Routing-Id"
  MSG_PROPERTY_SOCKET_TYPE  = "Socket-Type"
  MSG_PROPERTY_USER_ID      = "User-Id"
  MSG_PROPERTY_PEER_ADDRESS = "Peer-Address"

  NOTIFY_CONNECT    = 1
  NOTIFY_DISCONNECT = 2

  STREAMER  = 1
  FORWARDER = 2
  QUEUE     = 3

  OMQ_ARENA_THRESHOLD = 10_001

  DEFAULT_ARENA_THRESHOLD = 4 * 1024
  LAST_ENDPOINT_CAPACITY  = 512
  ZMQ_MSG_T_SIZE          =  64
  ETERM                   = 156_384_712 + 53
  START                   = Time.instant

  SOCKET_TYPES = {
    "pair"    => PAIR,
    "pub"     => PUB,
    "sub"     => SUB,
    "req"     => REQ,
    "rep"     => REP,
    "dealer"  => DEALER,
    "router"  => ROUTER,
    "pull"    => PULL,
    "push"    => PUSH,
    "xpub"    => XPUB,
    "xsub"    => XSUB,
    "stream"  => STREAM,
    "server"  => SERVER,
    "client"  => CLIENT,
    "radio"   => RADIO,
    "dish"    => DISH,
    "gather"  => GATHER,
    "scatter" => SCATTER,
    "dgram"   => DGRAM,
    "peer"    => PEER,
    "channel" => CHANNEL,
  }

  @[Link("omq_zmq")]
  lib LibZMQ
    alias FreeFn = (Void*, Void* ->)
    alias TimerFn = (LibC::Int, Void* ->)
    alias ThreadFn = (Void* ->)

    struct PollItem
      socket : Void*
      fd : LibC::Int
      events : LibC::Short
      revents : LibC::Short
    end

    struct PollerEvent
      socket : Void*
      fd : LibC::Int
      user_data : Void*
      events : LibC::Short
    end

    fun version = zmq_version(major : LibC::Int*, minor : LibC::Int*, patch : LibC::Int*) : Nil
    fun has = zmq_has(capability : LibC::Char*) : LibC::Int
    fun sleep = zmq_sleep(seconds : LibC::Int) : Nil
    fun ctx_new = zmq_ctx_new : Void*
    fun init = zmq_init(io_threads : LibC::Int) : Void*
    fun ctx_term = zmq_ctx_term(context : Void*) : LibC::Int
    fun term = zmq_term(context : Void*) : LibC::Int
    fun ctx_destroy = zmq_ctx_destroy(context : Void*) : LibC::Int
    fun ctx_shutdown = zmq_ctx_shutdown(context : Void*) : LibC::Int
    fun ctx_set = zmq_ctx_set(context : Void*, option : LibC::Int, value : LibC::Int) : LibC::Int
    fun ctx_get = zmq_ctx_get(context : Void*, option : LibC::Int) : LibC::Int
    fun ctx_set_ext = zmq_ctx_set_ext(context : Void*, option : LibC::Int, value : Void*, value_len : LibC::SizeT) : LibC::Int
    fun ctx_get_ext = zmq_ctx_get_ext(context : Void*, option : LibC::Int, value : Void*, value_len : LibC::SizeT*) : LibC::Int
    fun ctx_share_key = omq_ctx_share_key(context : Void*, high : UInt64*, low : UInt64*) : LibC::Int
    fun ctx_from_share_key = omq_ctx_from_share_key(high : UInt64, low : UInt64) : Void*

    fun socket = zmq_socket(context : Void*, socket_type : LibC::Int) : Void*
    fun close = zmq_close(socket : Void*) : LibC::Int
    fun bind = zmq_bind(socket : Void*, endpoint : LibC::Char*) : LibC::Int
    fun connect = zmq_connect(socket : Void*, endpoint : LibC::Char*) : LibC::Int
    fun unbind = zmq_unbind(socket : Void*, endpoint : LibC::Char*) : LibC::Int
    fun disconnect = zmq_disconnect(socket : Void*, endpoint : LibC::Char*) : LibC::Int
    fun join = zmq_join(socket : Void*, group : LibC::Char*) : LibC::Int
    fun leave = zmq_leave(socket : Void*, group : LibC::Char*) : LibC::Int
    fun connect_peer = zmq_connect_peer(socket : Void*, endpoint : LibC::Char*) : UInt32
    fun disconnect_peer = zmq_disconnect_peer(socket : Void*, routing_id : UInt32) : LibC::Int
    fun socket_monitor = zmq_socket_monitor(socket : Void*, endpoint : LibC::Char*, events : LibC::Int) : LibC::Int
    fun socket_monitor_versioned = zmq_socket_monitor_versioned(socket : Void*, endpoint : LibC::Char*, events : UInt64, event_version : LibC::Int, type : LibC::Int) : LibC::Int
    fun socket_monitor_pipes_stats = zmq_socket_monitor_pipes_stats(socket : Void*) : LibC::Int
    fun socket_get_peer_state = zmq_socket_get_peer_state(socket : Void*, routing_id : Void*, routing_id_size : LibC::SizeT) : LibC::Int

    fun setsockopt = zmq_setsockopt(socket : Void*, option : LibC::Int, value : Void*, value_len : LibC::SizeT) : LibC::Int
    fun getsockopt = zmq_getsockopt(socket : Void*, option : LibC::Int, value : Void*, value_len : LibC::SizeT*) : LibC::Int

    fun send = zmq_send(socket : Void*, data : Void*, len : LibC::SizeT, flags : LibC::Int) : LibC::Int
    fun send_const = zmq_send_const(socket : Void*, data : Void*, len : LibC::SizeT, flags : LibC::Int) : LibC::Int
    fun recv = zmq_recv(socket : Void*, data : Void*, len : LibC::SizeT, flags : LibC::Int) : LibC::Int
    fun sendmsg = zmq_sendmsg(socket : Void*, msg : Void*, flags : LibC::Int) : LibC::Int
    fun recvmsg = zmq_recvmsg(socket : Void*, msg : Void*, flags : LibC::Int) : LibC::Int
    fun sendiov = zmq_sendiov(socket : Void*, iov : Void*, count : LibC::SizeT, flags : LibC::Int) : LibC::Int
    fun recviov = zmq_recviov(socket : Void*, iov : Void*, count : LibC::SizeT*, flags : LibC::Int) : LibC::Int
    fun proxy = zmq_proxy(frontend : Void*, backend : Void*, capture : Void*) : LibC::Int
    fun proxy_steerable = zmq_proxy_steerable(frontend : Void*, backend : Void*, capture : Void*, control : Void*) : LibC::Int
    fun device = zmq_device(type : LibC::Int, frontend : Void*, backend : Void*) : LibC::Int
    fun poll = zmq_poll(items : PollItem*, nitems : LibC::Int, timeout_ms : LibC::Long) : LibC::Int
    fun ppoll = zmq_ppoll(items : PollItem*, nitems : LibC::Int, timeout_ms : LibC::Long, sigmask : Void*) : LibC::Int

    fun poller_new = zmq_poller_new : Void*
    fun poller_destroy = zmq_poller_destroy(poller : Void**) : LibC::Int
    fun poller_size = zmq_poller_size(poller : Void*) : LibC::Int
    fun poller_add = zmq_poller_add(poller : Void*, socket : Void*, user_data : Void*, events : LibC::Short) : LibC::Int
    fun poller_modify = zmq_poller_modify(poller : Void*, socket : Void*, events : LibC::Short) : LibC::Int
    fun poller_remove = zmq_poller_remove(poller : Void*, socket : Void*) : LibC::Int
    fun poller_wait = zmq_poller_wait(poller : Void*, event : PollerEvent*, timeout : LibC::Long) : LibC::Int
    fun poller_wait_all = zmq_poller_wait_all(poller : Void*, events : PollerEvent*, n_events : LibC::Int, timeout : LibC::Long) : LibC::Int
    fun poller_fd = zmq_poller_fd(poller : Void*, fd : LibC::Int*) : LibC::Int
    fun poller_add_fd = zmq_poller_add_fd(poller : Void*, fd : LibC::Int, user_data : Void*, events : LibC::Short) : LibC::Int
    fun poller_modify_fd = zmq_poller_modify_fd(poller : Void*, fd : LibC::Int, events : LibC::Short) : LibC::Int
    fun poller_remove_fd = zmq_poller_remove_fd(poller : Void*, fd : LibC::Int) : LibC::Int

    fun msg_init = zmq_msg_init(msg : Void*) : LibC::Int
    fun msg_init_size = zmq_msg_init_size(msg : Void*, size : LibC::SizeT) : LibC::Int
    fun msg_init_buffer = zmq_msg_init_buffer(msg : Void*, data : Void*, size : LibC::SizeT) : LibC::Int
    fun msg_init_data = zmq_msg_init_data(msg : Void*, data : Void*, size : LibC::SizeT, ffn : FreeFn, hint : Void*) : LibC::Int
    fun msg_recv = zmq_msg_recv(msg : Void*, socket : Void*, flags : LibC::Int) : LibC::Int
    fun msg_send = zmq_msg_send(msg : Void*, socket : Void*, flags : LibC::Int) : LibC::Int
    fun msg_close = zmq_msg_close(msg : Void*) : LibC::Int
    fun msg_move = zmq_msg_move(dest : Void*, src : Void*) : LibC::Int
    fun msg_copy = zmq_msg_copy(dest : Void*, src : Void*) : LibC::Int
    fun msg_data = zmq_msg_data(msg : Void*) : Void*
    fun msg_size = zmq_msg_size(msg : Void*) : LibC::SizeT
    fun msg_more = zmq_msg_more(msg : Void*) : LibC::Int
    fun msg_get = zmq_msg_get(msg : Void*, property : LibC::Int) : LibC::Int
    fun msg_set = zmq_msg_set(msg : Void*, property : LibC::Int, value : LibC::Int) : LibC::Int
    fun msg_gets = zmq_msg_gets(msg : Void*, property : LibC::Char*) : LibC::Char*
    fun msg_set_group = zmq_msg_set_group(msg : Void*, group : LibC::Char*) : LibC::Int
    fun msg_group = zmq_msg_group(msg : Void*) : LibC::Char*
    fun msg_set_routing_id = zmq_msg_set_routing_id(msg : Void*, routing_id : UInt32) : LibC::Int
    fun msg_routing_id = zmq_msg_routing_id(msg : Void*) : UInt32

    fun curve_keypair = zmq_curve_keypair(public_key : LibC::Char*, secret_key : LibC::Char*) : LibC::Int
    fun curve_public = zmq_curve_public(public_key : LibC::Char*, secret_key : LibC::Char*) : LibC::Int
    fun z85_encode = zmq_z85_encode(dest : LibC::Char*, data : UInt8*, size : LibC::SizeT) : LibC::Char*
    fun z85_decode = zmq_z85_decode(dest : UInt8*, string : LibC::Char*) : UInt8*

    fun atomic_counter_new = zmq_atomic_counter_new : Void*
    fun atomic_counter_set = zmq_atomic_counter_set(counter : Void*, value : LibC::Int) : Nil
    fun atomic_counter_inc = zmq_atomic_counter_inc(counter : Void*) : LibC::Int
    fun atomic_counter_dec = zmq_atomic_counter_dec(counter : Void*) : LibC::Int
    fun atomic_counter_value = zmq_atomic_counter_value(counter : Void*) : LibC::Int
    fun atomic_counter_destroy = zmq_atomic_counter_destroy(counter : Void**) : Nil

    fun timers_new = zmq_timers_new : Void*
    fun timers_destroy = zmq_timers_destroy(timers : Void**) : LibC::Int
    fun timers_add = zmq_timers_add(timers : Void*, interval : LibC::SizeT, handler : TimerFn, arg : Void*) : LibC::Int
    fun timers_cancel = zmq_timers_cancel(timers : Void*, timer_id : LibC::Int) : LibC::Int
    fun timers_set_interval = zmq_timers_set_interval(timers : Void*, timer_id : LibC::Int, interval : LibC::SizeT) : LibC::Int
    fun timers_reset = zmq_timers_reset(timers : Void*, timer_id : LibC::Int) : LibC::Int
    fun timers_timeout = zmq_timers_timeout(timers : Void*) : LibC::Long
    fun timers_execute = zmq_timers_execute(timers : Void*) : LibC::Int

    fun stopwatch_start = zmq_stopwatch_start : Void*
    fun stopwatch_intermediate = zmq_stopwatch_intermediate(watch : Void*) : LibC::ULong
    fun stopwatch_stop = zmq_stopwatch_stop(watch : Void*) : LibC::ULong
    fun threadstart = zmq_threadstart(func : ThreadFn, arg : Void*) : Void*
    fun threadclose = zmq_threadclose(thread : Void*) : Nil

    fun errno = zmq_errno : LibC::Int
    fun strerror = zmq_strerror(errno : LibC::Int) : LibC::Char*
  end

  class Error < Exception
    getter errno : Int32?

    def initialize(message : String, @errno : Int32? = nil)
      super(message)
    end
  end

  class Again < Error
  end

  class ClosedError < Error
  end

  class TerminatedError < Error
  end

  struct CurveKeypair
    getter public_key : String
    getter secret_key : String

    def initialize(@public_key : String, @secret_key : String)
    end
  end

  struct ShareKey
    getter high : UInt64
    getter low : UInt64

    def initialize(@high : UInt64, @low : UInt64)
    end
  end

  class Message
    getter parts : Array(Bytes)

    def initialize(parts : Array(String))
      @parts = parts.map(&.to_slice.dup)
    end

    def initialize(parts : Array(Bytes))
      @parts = parts.map(&.dup)
    end

    def self.single(part : String | Bytes) : Message
      new([part_to_bytes(part)])
    end

    def self.multipart(parts) : Message
      new(parts.map { |part| part_to_bytes(part) })
    end

    def self.route(identity : String | Bytes, body : Message) : Message
      new([part_to_bytes(identity), *body.parts])
    end

    def self.group(group : String | Bytes, body : String | Bytes) : Message
      new([part_to_bytes(group), part_to_bytes(body)])
    end

    def size : Int32
      @parts.size
    end

    def empty? : Bool
      @parts.empty?
    end

    def multipart? : Bool
      @parts.size > 1
    end

    def byte_size : Int32
      @parts.sum(&.size)
    end

    def part(index : Int) : Bytes?
      @parts[index]?.try(&.dup)
    end

    def [](index : Int) : Bytes
      @parts[index].dup
    end

    def string(index : Int = 0) : String
      part = @parts[index]? || Bytes.empty
      String.new(part)
    end

    def route : Bytes?
      part(0)
    end

    def group : String?
      return nil if @parts.empty?
      String.new(@parts[0])
    end

    def body : Message
      return Message.new([] of Bytes) if @parts.size <= 1
      Message.new(@parts[1..])
    end

    def to_a : Array(Bytes)
      @parts.map(&.dup)
    end

    private def self.part_to_bytes(part : String) : Bytes
      part.to_slice.dup
    end

    private def self.part_to_bytes(part : Bytes) : Bytes
      part.dup
    end
  end

  class PollItem
    getter socket : Socket
    getter events : Int16
    property revents : Int16

    def initialize(@socket : Socket, events : Int = POLLIN, @revents : Int16 = 0_i16)
      @events = events.to_i16
    end

    def readable? : Bool
      (@revents & POLLIN) != 0
    end

    def writable? : Bool
      (@revents & POLLOUT) != 0
    end
  end

  struct PollerEvent
    getter socket : Socket?
    getter events : Int16
    getter user_data : Pointer(Void)

    def initialize(@socket : Socket?, @events : Int16, @user_data : Pointer(Void))
    end

    def readable? : Bool
      (@events & POLLIN) != 0
    end

    def writable? : Bool
      (@events & POLLOUT) != 0
    end
  end

  def self.context(io_threads : Int = 1) : Context
    Context.new(io_threads)
  end

  def self.context_from_share_key(key : ShareKey) : Context
    raw = LibZMQ.ctx_from_share_key(key.high, key.low)
    raise last_error if raw.null?
    Context.new(raw)
  end

  def self.version : Tuple(Int32, Int32, Int32)
    major = 0
    minor = 0
    patch = 0
    LibZMQ.version(pointerof(major), pointerof(minor), pointerof(patch))
    {major.to_i32, minor.to_i32, patch.to_i32}
  end

  def self.has(capability : String) : Bool
    LibZMQ.has(capability.to_unsafe) != 0
  end

  def self.monotonic_seconds : Float64
    (Time.instant - START).total_seconds
  end

  def self.curve_keypair : CurveKeypair
    public_key = Bytes.new(41)
    secret_key = Bytes.new(41)
    OMQ.check_rc(LibZMQ.curve_keypair(public_key.to_unsafe.as(LibC::Char*), secret_key.to_unsafe.as(LibC::Char*)))
    CurveKeypair.new(String.new(public_key[0, 40]), String.new(secret_key[0, 40]))
  end

  def self.curve_public(secret_key : String) : String
    public_key = Bytes.new(41)
    OMQ.check_rc(LibZMQ.curve_public(public_key.to_unsafe.as(LibC::Char*), secret_key.to_unsafe))
    String.new(public_key[0, 40])
  end

  def self.z85_encode(data : Bytes) : String
    raise ArgumentError.new("Z85 input size must be divisible by 4") unless data.size.divisible_by?(4)
    out_bytes = Bytes.new(data.size // 4 * 5 + 1)
    ptr = LibZMQ.z85_encode(out_bytes.to_unsafe.as(LibC::Char*), data.to_unsafe, data.size)
    raise last_error if ptr.null?
    String.new(out_bytes[0, out_bytes.size - 1])
  end

  def self.z85_decode(value : String) : Bytes
    raise ArgumentError.new("Z85 string length must be divisible by 5") unless value.bytesize.divisible_by?(5)
    out_bytes = Bytes.new(value.bytesize // 5 * 4)
    ptr = LibZMQ.z85_decode(out_bytes.to_unsafe, value.to_unsafe)
    raise last_error if ptr.null?
    out_bytes
  end

  def self.poll(items : Array(PollItem), timeout_ms : Int = -1) : Int32
    native = items.map do |item|
      LibZMQ::PollItem.new(
        socket: item.socket.raw_pointer,
        fd: -1,
        events: item.events,
        revents: 0_i16
      )
    end
    rc = LibZMQ.poll(native.to_unsafe, native.size, timeout_ms.to_i64)
    raise last_error if rc < 0
    native.each_with_index { |item, index| items[index].revents = item.revents }
    rc.to_i32
  end

  def self.proxy(frontend : Socket, backend : Socket, capture : Socket? = nil) : Bool
    OMQ.check_rc(LibZMQ.proxy(frontend.raw_pointer, backend.raw_pointer, capture.try(&.raw_pointer) || Pointer(Void).null))
    true
  end

  def self.proxy_steerable(frontend : Socket, backend : Socket, capture : Socket? = nil, control : Socket? = nil) : Bool
    OMQ.check_rc(
      LibZMQ.proxy_steerable(
        frontend.raw_pointer,
        backend.raw_pointer,
        capture.try(&.raw_pointer) || Pointer(Void).null,
        control.try(&.raw_pointer) || Pointer(Void).null
      )
    )
    true
  end

  def self.socket_type_id(socket_type : Int) : Int32
    socket_type.to_i32
  end

  def self.socket_type_id(socket_type : String) : Int32
    SOCKET_TYPES[socket_type.downcase]? || raise ArgumentError.new("unknown socket type: #{socket_type}")
  end

  def self.check_rc(rc : Int)
    raise last_error if rc < 0
  end

  def self.last_error : Error
    errno = LibZMQ.errno
    message = String.new(LibZMQ.strerror(errno))
    case errno
    when LibC::EAGAIN
      Again.new(message, errno)
    when ETERM
      TerminatedError.new(message, errno)
    else
      Error.new(message, errno)
    end
  end

  class Context
    @raw : Pointer(Void)?
    @live_sockets = 0
    @mutex = Mutex.new

    def initialize(io_threads : Int = 1)
      raw = LibZMQ.ctx_new
      raise OMQ.last_error if raw.null?
      @raw = raw
      set(IO_THREADS, io_threads)
    rescue ex
      if raw && !raw.null?
        LibZMQ.ctx_term(raw)
      end
      raise ex
    end

    protected def initialize(raw : Pointer(Void))
      @raw = raw
    end

    def socket(socket_type, **options) : Socket
      context = reserve_socket
      raw = LibZMQ.socket(context, OMQ.socket_type_id(socket_type))
      if raw.null?
        release_socket
        raise OMQ.last_error
      end

      socket = Socket.new(self, raw)
      begin
        options.each { |key, value| apply_socket_option(socket, key, value) }
      rescue ex
        socket.close
        raise ex
      end
      socket
    end

    def share_key : ShareKey
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      high = 0_u64
      low = 0_u64
      OMQ.check_rc(LibZMQ.ctx_share_key(raw, pointerof(high), pointerof(low)))
      ShareKey.new(high, low)
    end

    def set(option : Int, value : Int) : Bool
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      OMQ.check_rc(LibZMQ.ctx_set(raw, option.to_i32, value.to_i32))
      true
    end

    def set_string(option : Int, value : String) : Bool
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      OMQ.check_rc(LibZMQ.ctx_set_ext(raw, option.to_i32, value.to_unsafe.as(Void*), value.bytesize))
      true
    end

    def set_bytes(option : Int, value : Bytes) : Bool
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      OMQ.check_rc(LibZMQ.ctx_set_ext(raw, option.to_i32, value.to_unsafe.as(Void*), value.size))
      true
    end

    def get(option : Int) : Int32
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      value = LibZMQ.ctx_get(raw, option.to_i32)
      raise OMQ.last_error if value < 0
      value.to_i32
    end

    def get_ext_i32(option : Int) : Int32
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      value = 0
      size = LibC::SizeT.new(sizeof(Int32))
      OMQ.check_rc(LibZMQ.ctx_get_ext(raw, option.to_i32, pointerof(value).as(Void*), pointerof(size)))
      value.to_i32
    end

    def get_ext_string(option : Int, capacity : Int = LAST_ENDPOINT_CAPACITY) : String
      bytes = get_ext_bytes(option, capacity)
      len = bytes.size
      len -= 1 if len > 0 && bytes[len - 1] == 0
      String.new(bytes[0, Math.max(len, 0)])
    end

    def get_ext_bytes(option : Int, capacity : Int = LAST_ENDPOINT_CAPACITY) : Bytes
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      buffer = Bytes.new(capacity)
      size = LibC::SizeT.new(buffer.size)
      OMQ.check_rc(LibZMQ.ctx_get_ext(raw, option.to_i32, buffer.to_unsafe.as(Void*), pointerof(size)))
      buffer[0, size.to_i].dup
    end

    def shutdown : Bool
      raw = @mutex.synchronize { @raw || raise ClosedError.new("context closed") }
      OMQ.check_rc(LibZMQ.ctx_shutdown(raw))
      true
    end

    def close : Bool
      raw = @mutex.synchronize do
        current = @raw
        return true unless current
        if @live_sockets > 0
          raise Error.new("context has #{@live_sockets} live sockets; close sockets before term()")
        end
        @raw = nil
        current
      end
      OMQ.check_rc(LibZMQ.ctx_term(raw))
      true
    rescue ex
      @mutex.synchronize { @raw ||= raw if raw }
      raise ex
    end

    def term : Bool
      close
    end

    def finalize
      raw = @mutex.synchronize do
        current = @raw
        @raw = nil
        current
      end
      LibZMQ.ctx_term(raw) if raw
    end

    protected def reserve_socket : Pointer(Void)
      @mutex.synchronize do
        raw = @raw
        raise ClosedError.new("context closed") unless raw
        @live_sockets += 1
        raw
      end
    end

    protected def release_socket : Nil
      @mutex.synchronize do
        raise Error.new("context live socket count underflow") if @live_sockets <= 0
        @live_sockets -= 1
      end
    end

    private def apply_socket_option(socket : Socket, key : Symbol, value)
      case key
      when :linger
        socket.set_linger(value)
      when :send_timeout, :sndtimeo
        socket.set_send_timeout(value)
      when :recv_timeout, :rcvtimeo
        socket.set_recv_timeout(value)
      when :send_hwm, :sndhwm
        socket.set_send_hwm(value)
      when :recv_hwm, :rcvhwm
        socket.set_recv_hwm(value)
      when :arena_threshold
        socket.set_arena_threshold(value)
      when :subscribe
        socket.subscribe(value)
      when :identity
        socket.set_identity(value)
      when :reconnect_interval
        socket.set_reconnect_interval(value)
      when :reconnect_interval_max
        socket.set_reconnect_interval_max(value)
      when :heartbeat_interval
        socket.set_heartbeat_interval(value)
      when :heartbeat_ttl
        socket.set_heartbeat_ttl(value)
      when :heartbeat_timeout
        socket.set_heartbeat_timeout(value)
      when :handshake_interval
        socket.set_handshake_interval(value)
      when :max_message_size
        socket.set_max_message_size(value)
      when :router_mandatory
        socket.set_router_mandatory(value)
      when :conflate
        socket.set_conflate(value)
      when :tcp_keepalive
        socket.set_tcp_keepalive(value)
      when :tcp_keepalive_count
        socket.set_tcp_keepalive_count(value)
      when :tcp_keepalive_idle
        socket.set_tcp_keepalive_idle(value)
      when :tcp_keepalive_interval
        socket.set_tcp_keepalive_interval(value)
      when :send_buffer
        socket.set_send_buffer(value)
      when :recv_buffer
        socket.set_recv_buffer(value)
      when :xpub_verbose
        socket.set_xpub_verbose(value)
      when :xpub_nodrop
        socket.set_xpub_nodrop(value)
      when :ipv6
        socket.set_ipv6(value)
      when :immediate
        socket.set_immediate(value)
      when :backlog
        socket.set_backlog(value)
      when :connect_timeout
        socket.set_connect_timeout(value)
      when :probe_router
        socket.set_probe_router(value)
      when :req_correlate
        socket.set_req_correlate(value)
      when :req_relaxed
        socket.set_req_relaxed(value)
      when :router_handover
        socket.set_router_handover(value)
      when :reconnect_stop
        socket.set_reconnect_stop(value)
      when :wss_key_pem
        socket.set_wss_key_pem(value)
      when :wss_cert_pem
        socket.set_wss_cert_pem(value)
      when :wss_trust_pem
        socket.set_wss_trust_pem(value)
      when :wss_hostname
        socket.set_wss_hostname(value)
      when :wss_trust_system
        socket.set_wss_trust_system(value)
      when :plain_server
        socket.set_plain_server(value)
      when :plain_username
        socket.set_plain_username(value)
      when :plain_password
        socket.set_plain_password(value)
      when :curve_server
        socket.set_curve_server(value)
      when :curve_public_key
        socket.set_curve_public_key(value)
      when :curve_secret_key
        socket.set_curve_secret_key(value)
      when :curve_server_key
        socket.set_curve_server_key(value)
      else
        raise ArgumentError.new("unknown socket option: #{key}")
      end
    end
  end

  class Socket
    @raw : Pointer(Void)?
    @mutex = Mutex.new

    protected def initialize(@context : Context, raw : Pointer(Void))
      @raw = raw
    end

    def raw_pointer : Pointer(Void)
      @mutex.synchronize { @raw || raise ClosedError.new("socket closed") }
    end

    def bind(endpoint : String) : String
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.bind(socket, endpoint.to_unsafe))
        last_endpoint(socket) || endpoint
      end
    end

    def connect(endpoint : String) : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.connect(socket, endpoint.to_unsafe)) }
      true
    end

    def unbind(endpoint : String) : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.unbind(socket, endpoint.to_unsafe)) }
      true
    end

    def disconnect(endpoint : String) : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.disconnect(socket, endpoint.to_unsafe)) }
      true
    end

    def connect_peer(endpoint : String) : UInt32
      routing_id = with_socket { |socket| LibZMQ.connect_peer(socket, endpoint.to_unsafe) }
      raise OMQ.last_error if routing_id == 0
      routing_id
    end

    def disconnect_peer(routing_id : UInt32) : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.disconnect_peer(socket, routing_id)) }
      true
    end

    def join(group : String) : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.join(socket, group.to_unsafe)) }
      true
    end

    def leave(group : String) : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.leave(socket, group.to_unsafe)) }
      true
    end

    def monitor(endpoint : String, events : Int = EVENT_ALL) : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.socket_monitor(socket, endpoint.to_unsafe, events.to_i32)) }
      true
    end

    def monitor_versioned(endpoint : String, events : Int = EVENT_ALL, event_version : Int = CURRENT_EVENT_VERSION, socket_type : Int = PAIR) : Bool
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.socket_monitor_versioned(socket, endpoint.to_unsafe, events.to_u64, event_version.to_i32, socket_type.to_i32))
      end
      true
    end

    def monitor_pipes_stats : Bool
      with_socket { |socket| OMQ.check_rc(LibZMQ.socket_monitor_pipes_stats(socket)) }
      true
    end

    def peer_state(routing_id : Bytes) : Int32
      with_socket do |socket|
        state = LibZMQ.socket_get_peer_state(socket, routing_id.to_unsafe.as(Void*), routing_id.size)
        raise OMQ.last_error if state < 0
        state.to_i32
      end
    end

    def close : Bool
      raw = @mutex.synchronize do
        current = @raw
        return true unless current
        @raw = nil
        current
      end
      begin
        OMQ.check_rc(LibZMQ.close(raw))
      ensure
        @context.release_socket
      end
      true
    end

    def finalize
      close
    rescue
    end

    def send(payload : String, flags : Int = 0) : Bool
      send_bytes(payload.to_slice, flags)
    end

    def send(payload : Bytes, flags : Int = 0) : Bool
      send_bytes(payload, flags)
    end

    def send_const(payload : String, flags : Int = 0) : Bool
      send_const_bytes(payload.to_slice, flags)
    end

    def send_const(payload : Bytes, flags : Int = 0) : Bool
      send_const_bytes(payload, flags)
    end

    def send(message : Message, flags : Int = 0) : Bool
      send_parts(message.parts, flags)
    end

    def send(parts : Array(String), flags : Int = 0) : Bool
      send_parts(parts, flags)
    end

    def send(parts : Array(Bytes), flags : Int = 0) : Bool
      send_parts(parts, flags)
    end

    def send_parts(parts, flags : Int = 0) : Bool
      raise ArgumentError.new("multipart send requires at least one part") if parts.empty?
      parts.each_with_index do |part, index|
        part_flags = index + 1 == parts.size ? flags : flags | SNDMORE
        send_bytes(part_to_bytes(part), part_flags)
      end
      true
    end

    def send_group(group : String, payload : String | Bytes, flags : Int = 0) : Bool
      payload_bytes = part_to_bytes(payload)
      msg = Bytes.new(ZMQ_MSG_T_SIZE)
      OMQ.check_rc(LibZMQ.msg_init_buffer(msg.to_unsafe.as(Void*), payload_bytes.to_unsafe.as(Void*), payload_bytes.size))
      begin
        OMQ.check_rc(LibZMQ.msg_set_group(msg.to_unsafe.as(Void*), group.to_unsafe))
        rc = with_socket { |socket| LibZMQ.msg_send(msg.to_unsafe.as(Void*), socket, flags.to_i32) }
        raise OMQ.last_error if rc < 0
      rescue ex
        LibZMQ.msg_close(msg.to_unsafe.as(Void*))
        raise ex
      end
      true
    end

    def recv(max_size : Int? = nil, flags : Int = 0) : String?
      recv_frame(max_size, flags).try { |frame| String.new(frame[0]) }
    end

    def recv_bytes(max_size : Int? = nil, flags : Int = 0) : Bytes?
      recv_frame(max_size, flags).try(&.[0])
    end

    def try_recv(max_size : Int? = nil) : String?
      recv(max_size, DONTWAIT)
    end

    def try_recv_bytes(max_size : Int? = nil) : Bytes?
      recv_bytes(max_size, DONTWAIT)
    end

    def recv_parts(max_size : Int? = nil, flags : Int = 0) : Array(String)
      recv_parts_bytes(max_size, flags).map { |part| String.new(part) }
    end

    def recv_parts_bytes(max_size : Int? = nil, flags : Int = 0) : Array(Bytes)
      parts = [] of Bytes
      loop do
        frame = recv_frame(max_size, flags)
        return parts unless frame
        payload, more = frame
        parts << payload
        break unless more
      end
      parts
    end

    def recv_message(max_size : Int? = nil, flags : Int = 0) : Message?
      parts = recv_parts_bytes(max_size, flags)
      return nil if parts.empty?
      Message.new(parts)
    end

    def subscribe(prefix) : Bool
      set_bytes(SUBSCRIBE, part_to_bytes(prefix))
    end

    def unsubscribe(prefix) : Bool
      set_bytes(UNSUBSCRIBE, part_to_bytes(prefix))
    end

    def set_linger(value) : Bool
      set_i32(LINGER, option_i32(value))
    end

    def set_send_timeout(value) : Bool
      set_i32(SNDTIMEO, option_i32(value))
    end

    def set_recv_timeout(value) : Bool
      set_i32(RCVTIMEO, option_i32(value))
    end

    def set_send_hwm(value) : Bool
      set_i32(SNDHWM, option_i32(value))
    end

    def set_recv_hwm(value) : Bool
      set_i32(RCVHWM, option_i32(value))
    end

    def set_arena_threshold(value) : Bool
      set_i64(OMQ_ARENA_THRESHOLD, option_i64(value))
    end

    def get_arena_threshold : Int64
      get_option_i64(OMQ_ARENA_THRESHOLD)
    end

    def set_identity(value) : Bool
      set_bytes(IDENTITY, part_to_bytes(value))
    end

    def get_identity : String
      get_option_string(IDENTITY)
    end

    def set_reconnect_interval(value) : Bool
      set_i32(RECONNECT_IVL, option_i32(value))
    end

    def set_reconnect_interval_max(value) : Bool
      set_i32(RECONNECT_IVL_MAX, option_i32(value))
    end

    def set_heartbeat_interval(value) : Bool
      set_i32(HEARTBEAT_IVL, option_i32(value))
    end

    def set_heartbeat_ttl(value) : Bool
      set_i32(HEARTBEAT_TTL, option_i32(value))
    end

    def set_heartbeat_timeout(value) : Bool
      set_i32(HEARTBEAT_TIMEOUT, option_i32(value))
    end

    def set_handshake_interval(value) : Bool
      set_i32(HANDSHAKE_IVL, option_i32(value))
    end

    def set_max_message_size(value) : Bool
      set_i64(MAXMSGSIZE, option_i64(value))
    end

    def set_router_mandatory(value) : Bool
      set_i32(ROUTER_MANDATORY, option_bool_i32(value))
    end

    def set_conflate(value) : Bool
      set_i32(CONFLATE, option_bool_i32(value))
    end

    def set_tcp_keepalive(value) : Bool
      set_i32(TCP_KEEPALIVE, option_i32(value))
    end

    def set_tcp_keepalive_count(value) : Bool
      set_i32(TCP_KEEPALIVE_CNT, option_i32(value))
    end

    def set_tcp_keepalive_idle(value) : Bool
      set_i32(TCP_KEEPALIVE_IDLE, option_i32(value))
    end

    def set_tcp_keepalive_interval(value) : Bool
      set_i32(TCP_KEEPALIVE_INTVL, option_i32(value))
    end

    def set_send_buffer(value) : Bool
      set_i32(SNDBUF, option_i32(value))
    end

    def set_recv_buffer(value) : Bool
      set_i32(RCVBUF, option_i32(value))
    end

    def set_xpub_verbose(value) : Bool
      set_i32(XPUB_VERBOSE, option_bool_i32(value))
    end

    def set_xpub_nodrop(value) : Bool
      set_i32(XPUB_NODROP, option_bool_i32(value))
    end

    def set_ipv6(value) : Bool
      set_i32(IPV6, option_bool_i32(value))
    end

    def set_immediate(value) : Bool
      set_i32(IMMEDIATE, option_bool_i32(value))
    end

    def set_backlog(value) : Bool
      set_i32(BACKLOG, option_i32(value))
    end

    def set_connect_timeout(value) : Bool
      set_i32(CONNECT_TIMEOUT, option_i32(value))
    end

    def set_probe_router(value) : Bool
      set_i32(PROBE_ROUTER, option_bool_i32(value))
    end

    def set_req_correlate(value) : Bool
      set_i32(REQ_CORRELATE, option_bool_i32(value))
    end

    def set_req_relaxed(value) : Bool
      set_i32(REQ_RELAXED, option_bool_i32(value))
    end

    def set_router_handover(value) : Bool
      set_i32(ROUTER_HANDOVER, option_bool_i32(value))
    end

    def set_reconnect_stop(value) : Bool
      set_i32(RECONNECT_STOP, option_i32(value))
    end

    def set_wss_key_pem(value) : Bool
      set_bytes(WSS_KEY_PEM, part_to_bytes(value))
    end

    def set_wss_cert_pem(value) : Bool
      set_bytes(WSS_CERT_PEM, part_to_bytes(value))
    end

    def set_wss_trust_pem(value) : Bool
      set_bytes(WSS_TRUST_PEM, part_to_bytes(value))
    end

    def set_wss_hostname(value) : Bool
      set_string(WSS_HOSTNAME, option_string(value))
    end

    def set_wss_trust_system(value) : Bool
      set_i32(WSS_TRUST_SYSTEM, option_bool_i32(value))
    end

    def set_plain_server(value) : Bool
      set_i32(PLAIN_SERVER, option_bool_i32(value))
    end

    def set_plain_username(value) : Bool
      set_string(PLAIN_USERNAME, option_string(value))
    end

    def set_plain_password(value) : Bool
      set_string(PLAIN_PASSWORD, option_string(value))
    end

    def set_plain_client(username : String, password : String) : Bool
      set_plain_username(username)
      set_plain_password(password)
      true
    end

    def set_curve_server(value) : Bool
      set_i32(CURVE_SERVER, option_bool_i32(value))
    end

    def set_curve_public_key(value) : Bool
      set_string(CURVE_PUBLICKEY, option_string(value))
    end

    def set_curve_secret_key(value) : Bool
      set_string(CURVE_SECRETKEY, option_string(value))
    end

    def set_curve_server_key(value) : Bool
      set_string(CURVE_SERVERKEY, option_string(value))
    end

    def set_curve_client(keypair : CurveKeypair, server_public_key : String) : Bool
      set_curve_public_key(keypair.public_key)
      set_curve_secret_key(keypair.secret_key)
      set_curve_server_key(server_public_key)
      true
    end

    def type : Int32
      get_option_i32(TYPE)
    end

    def events : Int32
      get_option_i32(EVENTS)
    end

    def last_endpoint : String
      get_option_string(LAST_ENDPOINT)
    end

    def get_option_i32(option : Int) : Int32
      with_socket { |socket| get_i32(socket, option) }
    end

    def get_option_i64(option : Int) : Int64
      with_socket { |socket| get_i64(socket, option) }
    end

    def get_option_string(option : Int, capacity : Int = LAST_ENDPOINT_CAPACITY) : String
      with_socket { |socket| get_string(socket, option, capacity) }
    end

    def get_option_bytes(option : Int, capacity : Int = LAST_ENDPOINT_CAPACITY) : Bytes
      with_socket { |socket| get_bytes(socket, option, capacity) }
    end

    def set_option_i32(option : Int, value : Int) : Bool
      set_i32(option, value)
    end

    def set_option_i64(option : Int, value : Int64) : Bool
      set_i64(option, value)
    end

    def set_option_string(option : Int, value : String) : Bool
      set_string(option, value)
    end

    def set_option_bytes(option : Int, value : Bytes) : Bool
      set_bytes(option, value)
    end

    private def with_socket(&)
      @mutex.synchronize do
        raw = @raw
        raise ClosedError.new("socket closed") unless raw
        yield raw
      end
    end

    private def send_bytes(payload : Bytes, flags : Int) : Bool
      with_socket do |socket|
        rc = LibZMQ.send(socket, payload.to_unsafe.as(Void*), payload.size, flags.to_i32)
        raise OMQ.last_error if rc < 0
      end
      true
    end

    private def send_const_bytes(payload : Bytes, flags : Int) : Bool
      with_socket do |socket|
        rc = LibZMQ.send_const(socket, payload.to_unsafe.as(Void*), payload.size, flags.to_i32)
        raise OMQ.last_error if rc < 0
      end
      true
    end

    private def recv_frame(max_size : Int?, flags : Int) : Tuple(Bytes, Bool)?
      with_socket do |socket|
        if limit = max_size
          recv_frame_bounded(socket, limit, flags)
        else
          recv_frame_msg(socket, flags)
        end
      end
    end

    private def recv_frame_bounded(socket : Pointer(Void), max_size : Int, flags : Int) : Tuple(Bytes, Bool)?
      raise ArgumentError.new("max_size must be non-negative") if max_size < 0
      scratch = Bytes.new(max_size)
      rc = LibZMQ.recv(socket, scratch.to_unsafe.as(Void*), scratch.size, flags.to_i32)
      if rc < 0
        errno = LibZMQ.errno
        return nil if errno == LibC::EAGAIN && (flags & DONTWAIT) != 0
        raise OMQ.last_error
      end
      size = rc.to_i
      if size > max_size
        raise Error.new("received message exceeded Crystal receive limit: size=#{size} limit=#{max_size}")
      end
      {scratch[0, size].dup, get_i32(socket, RCVMORE) != 0}
    end

    private def recv_frame_msg(socket : Pointer(Void), flags : Int) : Tuple(Bytes, Bool)?
      msg = Bytes.new(ZMQ_MSG_T_SIZE)
      OMQ.check_rc(LibZMQ.msg_init(msg.to_unsafe.as(Void*)))
      rc = LibZMQ.msg_recv(msg.to_unsafe.as(Void*), socket, flags.to_i32)
      if rc < 0
        errno = LibZMQ.errno
        LibZMQ.msg_close(msg.to_unsafe.as(Void*))
        return nil if errno == LibC::EAGAIN && (flags & DONTWAIT) != 0
        raise OMQ.last_error
      end

      size = LibZMQ.msg_size(msg.to_unsafe.as(Void*)).to_i
      data = LibZMQ.msg_data(msg.to_unsafe.as(Void*)).as(UInt8*)
      payload = Bytes.new(size)
      payload.copy_from(Slice.new(data, size)) if size > 0
      more = LibZMQ.msg_more(msg.to_unsafe.as(Void*)) != 0
      OMQ.check_rc(LibZMQ.msg_close(msg.to_unsafe.as(Void*)))
      {payload, more}
    end

    private def set_i32(option : Int, value : Int) : Bool
      raw_value = value.to_i32
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.setsockopt(socket, option.to_i32, pointerof(raw_value).as(Void*), sizeof(Int32)))
      end
      true
    end

    private def set_i64(option : Int, value : Int64) : Bool
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.setsockopt(socket, option.to_i32, pointerof(value).as(Void*), sizeof(Int64)))
      end
      true
    end

    private def set_string(option : Int, value : String) : Bool
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.setsockopt(socket, option.to_i32, value.to_unsafe.as(Void*), value.bytesize))
      end
      true
    end

    private def set_bytes(option : Int, value : Bytes) : Bool
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.setsockopt(socket, option.to_i32, value.to_unsafe.as(Void*), value.size))
      end
      true
    end

    private def get_i32(socket : Pointer(Void), option : Int) : Int32
      value = 0
      size = LibC::SizeT.new(sizeof(Int32))
      OMQ.check_rc(LibZMQ.getsockopt(socket, option.to_i32, pointerof(value).as(Void*), pointerof(size)))
      value.to_i32
    end

    private def get_i64(socket : Pointer(Void), option : Int) : Int64
      value = 0_i64
      size = LibC::SizeT.new(sizeof(Int64))
      OMQ.check_rc(LibZMQ.getsockopt(socket, option.to_i32, pointerof(value).as(Void*), pointerof(size)))
      value
    end

    private def get_string(socket : Pointer(Void), option : Int, capacity : Int) : String
      buffer, size = get_option_buffer(socket, option, capacity)
      len = size.to_i
      len -= 1 if len > 0 && buffer[len - 1] == 0
      String.new(buffer[0, Math.max(len, 0)])
    end

    private def get_bytes(socket : Pointer(Void), option : Int, capacity : Int) : Bytes
      buffer, size = get_option_buffer(socket, option, capacity)
      buffer[0, size.to_i].dup
    end

    private def get_option_buffer(socket : Pointer(Void), option : Int, capacity : Int) : Tuple(Bytes, LibC::SizeT)
      buffer = Bytes.new(capacity)
      size = LibC::SizeT.new(buffer.size)
      OMQ.check_rc(LibZMQ.getsockopt(socket, option.to_i32, buffer.to_unsafe.as(Void*), pointerof(size)))
      {buffer, size}
    end

    private def last_endpoint(socket : Pointer(Void)) : String?
      value = get_string(socket, LAST_ENDPOINT, LAST_ENDPOINT_CAPACITY)
      value.empty? ? nil : value
    end

    private def part_to_bytes(part : String) : Bytes
      part.to_slice
    end

    private def part_to_bytes(part : Bytes) : Bytes
      part
    end

    private def part_to_bytes(part) : Bytes
      raise ArgumentError.new("expected String or Bytes, got #{part.class}")
    end

    private def option_string(value : String) : String
      value
    end

    private def option_string(value) : String
      raise ArgumentError.new("expected String, got #{value.class}")
    end

    private def option_i32(value : Bool) : Int32
      value ? 1 : 0
    end

    private def option_i32(value : Int) : Int32
      value.to_i32
    end

    private def option_i32(value) : Int32
      raise ArgumentError.new("expected Int32-compatible value, got #{value.class}")
    end

    private def option_i64(value : Bool) : Int64
      value ? 1_i64 : 0_i64
    end

    private def option_i64(value : Int) : Int64
      value.to_i64
    end

    private def option_i64(value) : Int64
      raise ArgumentError.new("expected Int64-compatible value, got #{value.class}")
    end

    private def option_bool_i32(value : Bool) : Int32
      value ? 1 : 0
    end

    private def option_bool_i32(value : Int) : Int32
      value.to_i32
    end

    private def option_bool_i32(value) : Int32
      raise ArgumentError.new("expected Bool or Int32-compatible value, got #{value.class}")
    end
  end

  class Poller
    @raw : Pointer(Void)?
    @sockets = {} of UInt64 => Socket

    def initialize
      raw = LibZMQ.poller_new
      raise OMQ.last_error if raw.null?
      @raw = raw
    end

    def add(socket : Socket, events : Int = POLLIN, user_data : Pointer(Void) = Pointer(Void).null) : Bool
      raw = raw_pointer
      socket_ptr = socket.raw_pointer
      OMQ.check_rc(LibZMQ.poller_add(raw, socket_ptr, user_data, events.to_i16))
      @sockets[socket_ptr.address.to_u64] = socket
      true
    end

    def modify(socket : Socket, events : Int) : Bool
      OMQ.check_rc(LibZMQ.poller_modify(raw_pointer, socket.raw_pointer, events.to_i16))
      true
    end

    def remove(socket : Socket) : Bool
      socket_ptr = socket.raw_pointer
      OMQ.check_rc(LibZMQ.poller_remove(raw_pointer, socket_ptr))
      @sockets.delete(socket_ptr.address.to_u64)
      true
    end

    def add_fd(fd : Int, events : Int = POLLIN, user_data : Pointer(Void) = Pointer(Void).null) : Bool
      OMQ.check_rc(LibZMQ.poller_add_fd(raw_pointer, fd.to_i32, user_data, events.to_i16))
      true
    end

    def modify_fd(fd : Int, events : Int) : Bool
      OMQ.check_rc(LibZMQ.poller_modify_fd(raw_pointer, fd.to_i32, events.to_i16))
      true
    end

    def remove_fd(fd : Int) : Bool
      OMQ.check_rc(LibZMQ.poller_remove_fd(raw_pointer, fd.to_i32))
      true
    end

    def fd : Int32
      poller_fd = 0
      OMQ.check_rc(LibZMQ.poller_fd(raw_pointer, pointerof(poller_fd)))
      poller_fd.to_i32
    end

    def size : Int32
      rc = LibZMQ.poller_size(raw_pointer)
      raise OMQ.last_error if rc < 0
      rc.to_i32
    end

    def wait(timeout_ms : Int = -1) : PollerEvent?
      event = uninitialized LibZMQ::PollerEvent
      rc = LibZMQ.poller_wait(raw_pointer, pointerof(event), timeout_ms.to_i64)
      if rc < 0
        errno = LibZMQ.errno
        return nil if errno == LibC::EAGAIN
        raise OMQ.last_error
      end
      poller_event(event)
    end

    def wait_all(max_events : Int, timeout_ms : Int = -1) : Array(PollerEvent)
      raise ArgumentError.new("max_events must be positive") if max_events <= 0
      events = Array(LibZMQ::PollerEvent).new(max_events) { uninitialized LibZMQ::PollerEvent }
      rc = LibZMQ.poller_wait_all(raw_pointer, events.to_unsafe, max_events, timeout_ms.to_i64)
      if rc < 0
        errno = LibZMQ.errno
        return [] of PollerEvent if errno == LibC::EAGAIN
        raise OMQ.last_error
      end
      events[0, rc].map { |event| poller_event(event) }
    end

    def close : Bool
      raw = @raw
      return true unless raw
      poller_ptr = raw
      @raw = nil
      OMQ.check_rc(LibZMQ.poller_destroy(pointerof(poller_ptr)))
      @sockets.clear
      true
    end

    def finalize
      close
    rescue
    end

    private def raw_pointer : Pointer(Void)
      @raw || raise ClosedError.new("poller closed")
    end

    private def poller_event(event : LibZMQ::PollerEvent) : PollerEvent
      socket = event.socket.null? ? nil : @sockets[event.socket.address.to_u64]?
      PollerEvent.new(socket, event.events, event.user_data)
    end
  end
end
