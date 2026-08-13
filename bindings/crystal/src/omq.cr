require "json"

module OMQ
  VERSION = "0.1.0"

  PAIR   =  0
  PUB    =  1
  SUB    =  2
  REQ    =  3
  REP    =  4
  DEALER =  5
  ROUTER =  6
  PULL   =  7
  PUSH   =  8
  XPUB   =  9
  XSUB   = 10
  STREAM = 11

  DONTWAIT = 1
  SNDMORE  = 2

  SUBSCRIBE           =     6
  UNSUBSCRIBE         =     7
  RCVMORE             =    13
  LINGER              =    17
  SNDHWM              =    23
  RCVHWM              =    24
  RCVTIMEO            =    27
  SNDTIMEO            =    28
  LAST_ENDPOINT       =    32
  IO_THREADS          =     1
  OMQ_ARENA_THRESHOLD = 10001

  DEFAULT_ARENA_THRESHOLD = 4 * 1024
  LAST_ENDPOINT_CAPACITY  = 512
  ZMQ_MSG_T_SIZE          =  64
  ETERM                   = 156384712 + 53
  START                   = Time.instant

  SOCKET_TYPES = {
    "pair"   => PAIR,
    "pub"    => PUB,
    "sub"    => SUB,
    "req"    => REQ,
    "rep"    => REP,
    "dealer" => DEALER,
    "router" => ROUTER,
    "pull"   => PULL,
    "push"   => PUSH,
    "xpub"   => XPUB,
    "xsub"   => XSUB,
    "stream" => STREAM,
  }

  @[Link("omq_zmq")]
  lib LibZMQ
    fun ctx_new = zmq_ctx_new : Void*
    fun ctx_term = zmq_ctx_term(context : Void*) : LibC::Int
    fun ctx_set = zmq_ctx_set(context : Void*, option : LibC::Int, value : LibC::Int) : LibC::Int
    fun socket = zmq_socket(context : Void*, socket_type : LibC::Int) : Void*
    fun close = zmq_close(socket : Void*) : LibC::Int
    fun setsockopt = zmq_setsockopt(socket : Void*, option : LibC::Int, value : Void*, value_len : LibC::SizeT) : LibC::Int
    fun getsockopt = zmq_getsockopt(socket : Void*, option : LibC::Int, value : Void*, value_len : LibC::SizeT*) : LibC::Int
    fun bind = zmq_bind(socket : Void*, endpoint : LibC::Char*) : LibC::Int
    fun connect = zmq_connect(socket : Void*, endpoint : LibC::Char*) : LibC::Int
    fun send = zmq_send(socket : Void*, data : Void*, len : LibC::SizeT, flags : LibC::Int) : LibC::Int
    fun recv = zmq_recv(socket : Void*, data : Void*, len : LibC::SizeT, flags : LibC::Int) : LibC::Int
    fun msg_init = zmq_msg_init(msg : Void*) : LibC::Int
    fun msg_recv = zmq_msg_recv(msg : Void*, socket : Void*, flags : LibC::Int) : LibC::Int
    fun msg_close = zmq_msg_close(msg : Void*) : LibC::Int
    fun msg_data = zmq_msg_data(msg : Void*) : Void*
    fun msg_size = zmq_msg_size(msg : Void*) : LibC::SizeT
    fun msg_more = zmq_msg_more(msg : Void*) : LibC::Int
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

  def self.context(io_threads : Int = 1) : Context
    Context.new(io_threads)
  end

  def self.monotonic_seconds : Float64
    (Time.instant - START).total_seconds
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

    def socket(socket_type, *, linger : Int? = nil, send_timeout : Int? = nil,
               recv_timeout : Int? = nil, send_hwm : Int? = nil, recv_hwm : Int? = nil,
               arena_threshold : Int64 | Int32 | Int? = nil, subscribe : String | Bytes | Nil = nil) : Socket
      context = reserve_socket
      raw = LibZMQ.socket(context, OMQ.socket_type_id(socket_type))
      if raw.null?
        release_socket
        raise OMQ.last_error
      end

      socket = Socket.new(self, raw)
      begin
        socket.set_linger(linger) if linger
        socket.set_send_timeout(send_timeout) if send_timeout
        socket.set_recv_timeout(recv_timeout) if recv_timeout
        socket.set_send_hwm(send_hwm) if send_hwm
        socket.set_recv_hwm(recv_hwm) if recv_hwm
        socket.set_arena_threshold(arena_threshold) if arena_threshold
        socket.subscribe(subscribe) if subscribe
      rescue ex
        socket.close
        raise ex
      end
      socket
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

    private def set(option : Int, value : Int) : Nil
      raw = @raw || raise ClosedError.new("context closed")
      OMQ.check_rc(LibZMQ.ctx_set(raw, option.to_i32, value.to_i32))
    end
  end

  class Socket
    @raw : Pointer(Void)?
    @mutex = Mutex.new

    protected def initialize(@context : Context, raw : Pointer(Void))
      @raw = raw
    end

    def bind(endpoint : String) : String
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.bind(socket, endpoint.to_unsafe))
        last_endpoint(socket) || endpoint
      end
    end

    def connect(endpoint : String) : Bool
      with_socket do |socket|
        OMQ.check_rc(LibZMQ.connect(socket, endpoint.to_unsafe))
      end
      true
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

    def recv(max_size : Int? = nil, flags : Int = 0) : String?
      recv_frame(max_size, flags).try(&.[0])
    end

    def try_recv(max_size : Int? = nil) : String?
      recv(max_size, DONTWAIT)
    end

    def recv_parts(max_size : Int? = nil, flags : Int = 0) : Array(String)
      parts = [] of String
      loop do
        frame = recv_frame(max_size, flags)
        return parts unless frame
        payload, more = frame
        parts << payload
        break unless more
      end
      parts
    end

    def subscribe(prefix : String | Bytes) : Bool
      set_bytes(SUBSCRIBE, part_to_bytes(prefix))
    end

    def unsubscribe(prefix : String | Bytes) : Bool
      set_bytes(UNSUBSCRIBE, part_to_bytes(prefix))
    end

    def set_linger(value : Int) : Bool
      set_i32(LINGER, value)
    end

    def set_send_timeout(value : Int) : Bool
      set_i32(SNDTIMEO, value)
    end

    def set_recv_timeout(value : Int) : Bool
      set_i32(RCVTIMEO, value)
    end

    def set_send_hwm(value : Int) : Bool
      set_i32(SNDHWM, value)
    end

    def set_recv_hwm(value : Int) : Bool
      set_i32(RCVHWM, value)
    end

    def set_arena_threshold(value : Int) : Bool
      set_i64(OMQ_ARENA_THRESHOLD, value.to_i64)
    end

    def get_arena_threshold : Int64
      with_socket { |socket| get_i64(socket, OMQ_ARENA_THRESHOLD) }
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

    private def recv_frame(max_size : Int?, flags : Int) : Tuple(String, Bool)?
      with_socket do |socket|
        if limit = max_size
          recv_frame_bounded(socket, limit, flags)
        else
          recv_frame_msg(socket, flags)
        end
      end
    end

    private def recv_frame_bounded(socket : Pointer(Void), max_size : Int, flags : Int) : Tuple(String, Bool)?
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
      {String.new(scratch[0, size]), get_i32(socket, RCVMORE) != 0}
    end

    private def recv_frame_msg(socket : Pointer(Void), flags : Int) : Tuple(String, Bool)?
      msg = Bytes.new(ZMQ_MSG_T_SIZE)
      OMQ.check_rc(LibZMQ.msg_init(msg.to_unsafe.as(Void*)))
      rc = LibZMQ.msg_recv(msg.to_unsafe.as(Void*), socket, flags.to_i32)
      if rc < 0
        errno = LibZMQ.errno
        LibZMQ.msg_close(msg.to_unsafe.as(Void*))
        return nil if errno == LibC::EAGAIN && (flags & DONTWAIT) != 0
        raise Error.new(String.new(LibZMQ.strerror(errno)), errno)
      end

      size = LibZMQ.msg_size(msg.to_unsafe.as(Void*)).to_i
      data = LibZMQ.msg_data(msg.to_unsafe.as(Void*)).as(UInt8*)
      payload = String.new(Slice.new(data, size))
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

    private def last_endpoint(socket : Pointer(Void)) : String?
      buffer = Bytes.new(LAST_ENDPOINT_CAPACITY)
      size = LibC::SizeT.new(buffer.size)
      OMQ.check_rc(LibZMQ.getsockopt(socket, LAST_ENDPOINT, buffer.to_unsafe.as(Void*), pointerof(size)))
      len = size.to_i
      len -= 1 if len > 0 && buffer[len - 1] == 0
      return nil if len <= 0
      String.new(buffer[0, len])
    end

    private def part_to_bytes(part : String) : Bytes
      part.to_slice
    end

    private def part_to_bytes(part : Bytes) : Bytes
      part
    end
  end
end
