# frozen_string_literal: true

require "io/wait"

module OMQ
  # OMQ.rs-backed Ruby socket API.
  module Rust
    # Supported socket type names.
    # @return [Array<Symbol>]
    SOCKET_TYPES = %i[
      req rep pub sub xpub xsub push pull dealer router pair stream
      client server radio dish scatter gather channel peer
    ].freeze

    # @api private
    ROUTED_TYPES = %i[server].freeze

    # @api private
    SINGLE_FRAME_TYPES = %i[client server scatter gather channel].freeze

    # @api private
    SOCKET_OPTIONS = %w[
      workload_profile
      send_hwm
      recv_hwm
      recv_rate_limit
      recv_ip_rate_limit

      linger
      identity
      router_mandatory
      conflate

      heartbeat_interval
      heartbeat_ttl
      heartbeat_timeout
      handshake_timeout
      max_pending_handshakes

      max_message_size
      sndbuf
      rcvbuf
      large_message_threshold
      arena_threshold
      transmit_slot_cap

      xpub_nodrop
      reconnect_stop_conn_refused
      on_mute
      reconnect_interval
      reconnect_interval_min
      reconnect_interval_max

      compression_dict
      compression_auto_train
      compression_threshold
      compression_level
      compression_dict_capacity
      max_recv_dict_size
      compression_offload_threshold

      mechanism_type
      mechanism_server
      mechanism_public_key
      mechanism_secret_key
      mechanism_server_key
      mechanism_username
      mechanism_password

      curve_server
      curve_publickey
      curve_public_key
      curve_secretkey
      curve_secret_key
      curve_serverkey
      curve_server_key

      plain_server
      plain_username
      plain_password
    ].freeze
    private_constant :ROUTED_TYPES, :SINGLE_FRAME_TYPES, :SOCKET_OPTIONS

    # CURVE peer metadata passed to callable authenticators.
    #
    # @!attribute [r] public_key
    #   @return [String] peer's 40-byte Z85 public key
    # @!attribute [r] identity
    #   @return [String, nil] peer's ZMTP identity
    MechanismPeerInfo = Data.define(:public_key, :identity)

    class << self
      # Returns number of OMQ.rs IO threads.
      #
      # @return [Integer]
      def io_threads
        Native.io_threads
      end

      # Sets number of OMQ.rs IO threads before the shared runtime starts.
      #
      # @param count [Integer] positive IO thread count
      # @return [Integer] assigned count
      # @raise [ArgumentError] if +count+ is not positive
      # @note Set this before materializing any socket. It does not resize a
      #   running runtime.
      def io_threads=(count)
        count = Integer(count)
        raise ArgumentError, "io_threads must be positive" unless count.positive?

        Native.send(:io_threads=, count)
      end

      # Creates a socket for a named OMQ pattern.
      #
      # @param socket_type [Symbol, String] socket pattern, such as +:pull+
      # @param options [Hash] native socket options
      # @return [Socket] concrete socket instance
      # @raise [ArgumentError] if +socket_type+ is unknown or an option is invalid
      def socket(socket_type, **options)
        type = socket_type.to_s.downcase.to_sym
        unless SOCKET_TYPES.include?(type)
          raise ArgumentError, "unknown socket type: #{socket_type}"
        end

        const_get(type.to_s.upcase).new(**options)
      end

      # Reports whether binding was compiled with a feature.
      #
      # @param feature [Symbol, String] feature name, such as +:curve+ or +:zstd+
      # @return [Boolean]
      def has(feature)
        Native.has(feature.to_s)
      end

      # Generates a CurveZMQ keypair.
      #
      # @return [Array<String>] public and secret 40-byte Z85 keys
      def curve_keypair
        Native.curve_keypair
      end

      # Derives CurveZMQ public key from a secret key.
      #
      # @param secret_key [String] 40-byte Z85 secret key
      # @return [String] 40-byte Z85 public key
      # @raise [ArgumentError] if +secret_key+ is invalid
      def curve_public(secret_key)
        Native.curve_public(secret_key)
      end

      # Adapts a public CURVE authenticator to native peer metadata.
      #
      # @param authenticator [#call] callable receiving {MechanismPeerInfo}
      # @return [Proc]
      # @api private
      def wrap_curve_authenticator(authenticator)
        proc do |peer|
          authenticator.call(
            MechanismPeerInfo.new(
              public_key: peer.fetch(:public_key),
              identity: peer[:identity],
            ),
          )
        end
      end
    end

    # Enumerable stream of socket lifecycle events.
    class Monitor
      include Enumerable

      # Creates a monitor for a socket.
      #
      # Normally obtained through {Socket#monitor}.
      #
      # @param socket [Socket]
      # @return [Monitor]
      def initialize(socket)
        @socket = socket
      end

      # Receives next monitor event.
      #
      # @param timeout [Numeric, nil] maximum wait in seconds
      # @return [Hash, nil] event fields, or +nil+ when socket closes
      # @raise [IO::TimeoutError] if timeout expires
      def recv(timeout: nil)
        @socket.monitor_event(timeout: timeout)
      end

      # Receives next available monitor event without blocking.
      #
      # @return [Hash, nil]
      def recv_nowait
        @socket.try_monitor_event
      end

      # Yields monitor events until socket closes.
      #
      # @yieldparam event [Hash]
      # @return [Enumerator, nil] enumerator without a block
      def each
        return enum_for(__method__) unless block_given?

        while (event = recv)
          yield event
        end
      rescue IOError
        raise unless @socket.closed?
      end
    end

    # Base class for OMQ.rs-backed sockets.
    class Socket
      # @return [Symbol] lowercase socket pattern
      attr_reader :socket_type

      # Creates a socket without binding or connecting it.
      #
      # @param recv_timeout [Numeric, nil] receive timeout in seconds
      # @param send_timeout [Numeric, nil] send timeout in seconds
      # @param curve_auth [Array<String>, #call, nil] CURVE allowlist or authenticator
      # @param options [Hash] native OMQ.rs socket options
      # @option options [Symbol] :workload_profile +:throughput+ or +:latency+
      # @option options [Integer] :send_hwm outbound message capacity
      # @option options [Integer] :recv_hwm inbound message capacity
      # @option options [Hash] :recv_rate_limit per-connection
      #   +:messages_per_second+ (or +:rate+) and +:burst+
      # @option options [Hash] :recv_ip_rate_limit per-IP
      #   +:messages_per_second+ (or +:rate+) and +:burst+
      # @option options [Numeric] :linger close linger in seconds; positive
      #   infinity waits forever
      # @option options [String] :identity ZMTP socket identity
      # @option options [Boolean] :router_mandatory reject unroutable sends
      # @option options [Boolean] :conflate retain only latest received message
      # @option options [Numeric] :heartbeat_interval heartbeat period in seconds
      # @option options [Numeric] :heartbeat_ttl remote heartbeat TTL in seconds
      # @option options [Numeric] :heartbeat_timeout peer timeout in seconds
      # @option options [Numeric] :handshake_timeout handshake timeout in seconds
      # @option options [Integer] :max_pending_handshakes inbound handshake limit
      # @option options [Integer] :max_message_size maximum received message bytes
      # @option options [Integer] :sndbuf kernel send buffer bytes
      # @option options [Integer] :rcvbuf kernel receive buffer bytes
      # @option options [Integer] :large_message_threshold receive buffer threshold
      # @option options [Integer] :arena_threshold contiguous frame arena threshold
      # @option options [Integer] :transmit_slot_cap per-peer transmit bytes
      # @option options [Boolean] :xpub_nodrop block instead of dropping on mute
      # @option options [Boolean] :reconnect_stop_conn_refused stop refused reconnects
      # @option options [Symbol] :on_mute +:block+, +:drop_newest+, or +:drop_oldest+
      # @option options [Numeric] :reconnect_interval fixed reconnect delay in seconds
      # @option options [Numeric] :reconnect_interval_min minimum backoff in seconds
      # @option options [Numeric] :reconnect_interval_max maximum backoff in seconds
      # @option options [String] :compression_dict compression dictionary bytes
      # @option options [Boolean] :compression_auto_train train a zstd dictionary
      # @option options [Integer] :compression_threshold minimum bytes to compress
      # @option options [Integer] :compression_level zstd compression level
      # @option options [Integer] :compression_dict_capacity trained dictionary bytes
      # @option options [Integer] :max_recv_dict_size received dictionary byte limit
      # @option options [Integer] :compression_offload_threshold offload threshold;
      #   negative disables offloading
      # @option options [Symbol] :mechanism_type +:null+, +:plain+, or +:curve+
      # @option options [Boolean] :plain_server enable PLAIN server mode
      # @option options [String] :plain_username PLAIN client username
      # @option options [String] :plain_password PLAIN client password
      # @option options [Boolean] :curve_server enable CURVE server mode
      # @option options [String] :curve_publickey local raw or Z85 public key
      # @option options [String] :curve_secretkey local raw or Z85 secret key
      # @option options [String] :curve_serverkey CURVE server raw or Z85 public key
      # @return [Socket]
      # @raise [ArgumentError] if an option is unknown or invalid
      def initialize(recv_timeout: nil, send_timeout: nil, curve_auth: nil, **options)
        socket_type = self.class.const_get(:SOCKET_TYPE, false)
        @socket_type = socket_type.to_s.downcase.to_sym
        unless SOCKET_TYPES.include?(@socket_type)
          raise ArgumentError, "unknown socket type: #{socket_type}"
        end

        @recv_timeout = recv_timeout
        @send_timeout = send_timeout
        @request_waiting = false
        @reply_ready     = false
        @native       = Native::Socket.new(@socket_type.to_s.upcase)
        @native.set_options(normalize_options(options))
        @materialize_lock = Mutex.new
        @materialized = false
        @recv_io = nil
        @send_io = nil
        @peer_connected = false
        @subscriber_joined = false
        set_curve_auth(curve_auth) unless curve_auth.nil?
      end

      # Binds socket to an endpoint.
      #
      # @param endpoint [String, #to_str]
      # @return [String] resolved endpoint, including assigned ephemeral port
      def bind(endpoint)
        ensure_materialized
        @native.bind(String(endpoint))
      end

      # Connects socket to an endpoint.
      #
      # @param endpoint [String, #to_str]
      # @return [Socket] self
      def connect(endpoint)
        ensure_materialized
        @native.connect(String(endpoint))
        self
      end

      # Disconnects socket from an endpoint.
      #
      # @param endpoint [String, #to_str]
      # @return [Socket] self
      def disconnect(endpoint)
        ensure_materialized
        @native.disconnect(String(endpoint))
        self
      end

      # Stops listening on an endpoint.
      #
      # @param endpoint [String, #to_str]
      # @return [Socket] self
      def unbind(endpoint)
        ensure_materialized
        @native.unbind(String(endpoint))
        self
      end

      # Returns metadata for live SERVER route.
      #
      # @param routing_id [Integer] SERVER routing ID
      # @return [Hash, nil] peer metadata, or +nil+ for stale route
      # @raise [RuntimeError] unless called on SERVER socket
      def peer_info(routing_id)
        ensure_materialized
        @native.peer_info(routing_id)
      end

      # Configures CURVE client authorization on a server before materialization.
      #
      # @param authenticator [Array<String>, #call, nil] public-key allowlist,
      #   callable receiving {MechanismPeerInfo}, or +nil+ to allow valid clients
      # @yieldparam peer [MechanismPeerInfo]
      # @return [Socket] self
      # @raise [RuntimeError] if socket is already materialized
      # @raise [TypeError] if authenticator is unsupported
      def set_curve_auth(authenticator = nil, &block)
        raise RuntimeError, "CURVE authentication must be configured before bind or connect" if @materialized
        authenticator = block if block

        case authenticator
        when nil
          @native.clear_curve_auth
        when Array
          @native.set_curve_auth_keys(authenticator)
        else
          unless authenticator.respond_to?(:call)
            raise TypeError, "CURVE authenticator must be an Array, callable, or nil"
          end

          @native.set_curve_auth_callback(Rust.wrap_curve_authenticator(authenticator))
        end
        self
      end

      # Sends message, blocking while send queue is full.
      #
      # @param message [String, Integer, Array] first frame or complete message
      # @param more [Array<String, Integer>] additional frames
      # @return [Socket] self
      # @raise [IO::TimeoutError] if send timeout expires
      # @raise [ArgumentError, RuntimeError] if message violates socket pattern
      def send(message, *more)
        ensure_materialized
        parts = normalize_parts(message, more)
        validate_send_parts!(parts)
        validate_pattern_state_before_send!

        loop do
          result = enqueue(parts)
          if result == :ok
            sent!
            return self
          end

          wait_for(@send_io, @send_timeout, "send timed out")
          raise IOError, "socket closed" if closed?
        end
      end
      # Sends message using {#send}.
      # @see #send
      alias << send

      # Attempts to send without blocking.
      #
      # @param message [String, Integer, Array] first frame or complete message
      # @param more [Array<String, Integer>] additional frames
      # @return [Boolean] whether message was queued
      # @raise [ArgumentError, RuntimeError] if message violates socket pattern
      def try_send(message, *more)
        ensure_materialized
        parts = normalize_parts(message, more)
        validate_send_parts!(parts)
        validate_pattern_state_before_send!
        return false unless enqueue(parts) == :ok

        sent!
        true
      end

      # Receives next message.
      #
      # @return [Array<String, Integer>] message frames; SERVER prepends routing ID
      # @raise [IO::TimeoutError] if receive timeout expires
      # @raise [IOError] if socket closes
      def recv
        ensure_materialized
        message = try_recv
        return message if message

        loop do
          wait_for(@recv_io, @recv_timeout, "receive timed out")
          message = try_recv
          return message if message
          raise IOError, "socket closed" if closed?
        end
      end
      # Receives next message using {#recv}.
      # @see #recv
      alias receive recv

      # Attempts to receive without blocking.
      #
      # @return [Array<String, Integer>, nil] next message, or +nil+ if none is ready
      def try_recv
        ensure_materialized

        message = if ROUTED_TYPES.include?(@socket_type)
          @native.try_recv_routed
        else
          @native.try_recv
        end
        received! if message
        message
      end

      # Waits for receive notification.
      #
      # Notification may represent a message, close, or explicit {#wake_recv}.
      #
      # @param timeout [Numeric, nil] maximum wait in seconds
      # @return [true]
      # @raise [IO::TimeoutError] if timeout expires
      def wait_readable(timeout: @recv_timeout)
        ensure_materialized
        wait_for(@recv_io, timeout, "receive timed out")
        true
      end

      # Wakes a thread or fiber blocked in {#wait_readable}.
      #
      # @return [Socket] self
      def wake_recv
        @native.wake_recv if @materialized
        self
      end

      # Yields received messages until socket closes.
      #
      # @yieldparam message [Array<String, Integer>]
      # @return [Enumerator, nil] enumerator without a block
      def each
        return enum_for(__method__) unless block_given?

        loop { yield recv }
      rescue IOError
        raise unless closed?
      end

      # Adds SUB or XSUB subscription prefix.
      #
      # @param prefix [String, #to_str]
      # @return [Socket] self
      def subscribe(prefix = "")
        ensure_materialized
        @native.subscribe(String(prefix).b)
        self
      end

      # Removes SUB or XSUB subscription prefix.
      #
      # @param prefix [String, #to_str]
      # @return [Socket] self
      def unsubscribe(prefix = "")
        ensure_materialized
        @native.unsubscribe(String(prefix).b)
        self
      end

      # Joins DISH group.
      #
      # @param group [String, #to_str]
      # @return [Socket] self
      def join(group)
        ensure_materialized
        @native.join(String(group).b)
        self
      end

      # Leaves DISH group.
      #
      # @param group [String, #to_str]
      # @return [Socket] self
      def leave(group)
        ensure_materialized
        @native.leave(String(group).b)
        self
      end

      # Publishes RADIO message to group.
      #
      # @param group [String, #to_str]
      # @param message [String, #to_str]
      # @return [Socket] self
      def publish(group, message)
        send(group, message)
      end

      # Waits until first peer completes handshake.
      #
      # @param timeout [Numeric, nil] maximum wait in seconds
      # @return [Socket] self
      # @raise [IO::TimeoutError] if timeout expires
      # @raise [IOError] if socket closes first
      def wait_for_peer(timeout: nil)
        raise IOError, "socket closed" if closed?
        return self if @peer_connected

        ensure_materialized
        wait_for_native_fd(@native.peer_connected_fd, timeout, "peer connection timed out")
        raise IOError, "socket closed" if closed?

        @peer_connected = true
        self
      end

      # Waits until PUB, XPUB, or RADIO receives first subscription.
      #
      # @param timeout [Numeric, nil] maximum wait in seconds
      # @return [Socket] self
      # @raise [IO::TimeoutError] if timeout expires
      # @raise [IOError] if socket closes first
      def wait_for_subscriber(timeout: nil)
        raise IOError, "socket closed" if closed?
        return self if @subscriber_joined

        ensure_materialized
        wait_for_native_fd(@native.subscriber_joined_fd, timeout, "subscriber timed out")
        raise IOError, "socket closed" if closed?

        @subscriber_joined = true
        self
      end

      # Returns socket lifecycle monitor.
      #
      # @return [Monitor]
      def monitor
        ensure_materialized
        @monitor ||= Monitor.new(self)
      end

      # Returns monitor notification file descriptor.
      #
      # Intended for event-loop adapters; use {#monitor} otherwise.
      #
      # @return [Integer]
      def monitor_fd
        ensure_materialized
        @native.monitor_fd
      end

      # Receives next monitor event.
      #
      # @param timeout [Numeric, nil] maximum wait in seconds
      # @return [Hash, nil]
      # @raise [IO::TimeoutError] if timeout expires
      def monitor_event(timeout: @recv_timeout)
        return if closed?

        ensure_materialized
        event = @native.try_recv_monitor
        return event if event

        wait_for_native_fd(@native.monitor_fd, timeout, "monitor receive timed out")
        @native.try_recv_monitor
      end

      # Attempts to receive monitor event without blocking.
      #
      # @return [Hash, nil]
      def try_monitor_event
        return if closed?

        ensure_materialized
        @native.try_recv_monitor
      end

      # Closes socket and releases native resources.
      #
      # @return [nil]
      def close
        return if closed?

        @native.close
        close_wrapper(@recv_io)
        close_wrapper(@send_io)
        nil
      end

      # Reports whether socket is closed.
      #
      # @return [Boolean]
      def closed?
        @native.closed?
      end

      private

      def ensure_materialized
        return if @materialized

        @materialize_lock.synchronize do
          return if @materialized
          raise IOError, "socket closed" if closed?

          @native.materialize
          @recv_io = IO.for_fd(@native.recv_fd, autoclose: false)
          @send_io = IO.for_fd(@native.send_fd, autoclose: false)
          @materialized = true
        end
      end

      def normalize_parts(message, more)
        parts = if more.empty? && message.is_a?(Array)
          message
        else
          [message, *more]
        end
        parts.map { |part| part.is_a?(Integer) ? part : String(part).b }
      end

      def normalize_options(options)
        normalized = options.to_h do |key, value|
          value = value.to_s if value.is_a?(Symbol)
          value = value.transform_keys(&:to_s) if value.is_a?(Hash)
          [key.to_s, value]
        end
        unknown = normalized.keys - SOCKET_OPTIONS
        unless unknown.empty?
          raise ArgumentError, "unknown socket option#{"s" if unknown.length > 1}: #{unknown.sort.join(", ")}"
        end

        normalized
      end

      def validate_send_parts!(parts)
        if ROUTED_TYPES.include?(@socket_type)
          unless parts.length == 2 && parts[0].is_a?(Integer)
            raise ArgumentError, "#{@socket_type.upcase} send requires [routing_id, body]"
          end
        elsif SINGLE_FRAME_TYPES.include?(@socket_type) && parts.length != 1
          raise ArgumentError, "#{@socket_type.upcase} sockets require one message frame"
        elsif @socket_type == :radio && parts.length != 2
          raise ArgumentError, "RADIO send requires [group, body]"
        elsif @socket_type == :stream && parts.length != 2
          raise ArgumentError, "STREAM send requires [routing_id, body]"
        end
      end

      def validate_pattern_state_before_send!
        if @socket_type == :req && @request_waiting
          raise RuntimeError, "REQ must receive before sending again"
        end
        if @socket_type == :rep && !@reply_ready
          raise RuntimeError, "REP must receive before sending"
        end
      end

      def sent!
        @request_waiting = true if @socket_type == :req
        @reply_ready = false if @socket_type == :rep
      end

      def received!
        @request_waiting = false if @socket_type == :req
        @reply_ready = true if @socket_type == :rep
      end

      def enqueue(parts)
        if ROUTED_TYPES.include?(@socket_type)
          routing_id, body = parts
          @native.enqueue_send_routed([body], routing_id)
        else
          @native.enqueue_send(parts)
        end
      end

      def wait_for(io, timeout, message)
        ready = io.wait_readable(timeout)
        raise IO::TimeoutError, message unless ready

        drain(io)
      rescue Errno::EBADF
        raise IOError, "socket closed" if closed?

        raise
      end

      def wait_for_native_fd(fd, timeout, message)
        io = IO.for_fd(fd, autoclose: false)
        wait_for(io, timeout, message)
      ensure
        close_wrapper(io)
      end

      def drain(io)
        loop do
          result = io.read_nonblock(256, exception: false)
          break if result == :wait_readable || result.nil? || result.empty?
        end
      end

      def close_wrapper(io)
        io.close if io && !io.closed?
      rescue IOError, SystemCallError
      end
    end

    # @!parse
    #   # REQ socket.
    #   class REQ < Socket; end
    #   # REP socket.
    #   class REP < Socket; end
    #   # PUB socket.
    #   class PUB < Socket; end
    #   # SUB socket.
    #   class SUB < Socket; end
    #   # XPUB socket.
    #   class XPUB < Socket; end
    #   # XSUB socket.
    #   class XSUB < Socket; end
    #   # PUSH socket.
    #   class PUSH < Socket; end
    #   # PULL socket.
    #   class PULL < Socket; end
    #   # DEALER socket.
    #   class DEALER < Socket; end
    #   # ROUTER socket.
    #   class ROUTER < Socket; end
    #   # PAIR socket.
    #   class PAIR < Socket; end
    #   # STREAM socket.
    #   class STREAM < Socket; end
    #   # CLIENT socket.
    #   class CLIENT < Socket; end
    #   # SERVER socket.
    #   class SERVER < Socket; end
    #   # RADIO socket.
    #   class RADIO < Socket; end
    #   # DISH socket.
    #   class DISH < Socket; end
    #   # SCATTER socket.
    #   class SCATTER < Socket; end
    #   # GATHER socket.
    #   class GATHER < Socket; end
    #   # CHANNEL socket.
    #   class CHANNEL < Socket; end
    #   # PEER socket.
    #   class PEER < Socket; end
    SOCKET_TYPES.each do |type|
      klass = Class.new(Socket)
      klass.const_set(:SOCKET_TYPE, type)
      const_set(type.to_s.upcase, klass)
    end
  end
end
