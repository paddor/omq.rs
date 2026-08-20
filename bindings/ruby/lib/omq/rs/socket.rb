# frozen_string_literal: true

require "io/wait"

module OMQ
  module Rust
    SOCKET_TYPES = %i[
      req rep pub sub xpub xsub push pull dealer router pair stream
      client server radio dish scatter gather channel peer
    ].freeze
    ROUTED_TYPES = %i[server].freeze
    SINGLE_FRAME_TYPES = %i[client server scatter gather channel].freeze
    MechanismPeerInfo = Data.define(:public_key, :identity)

    class << self
      def io_threads
        Native.io_threads
      end

      def io_threads=(count)
        count = Integer(count)
        raise ArgumentError, "io_threads must be positive" unless count.positive?

        Native.send(:io_threads=, count)
      end

      def socket(socket_type, **options)
        type = socket_type.to_s.downcase.to_sym
        unless SOCKET_TYPES.include?(type)
          raise ArgumentError, "unknown socket type: #{socket_type}"
        end

        const_get(type.to_s.upcase).new(**options)
      end

      def has(feature)
        Native.has(feature.to_s)
      end

      def curve_keypair
        Native.curve_keypair
      end

      def curve_public(secret_key)
        Native.curve_public(secret_key)
      end

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

    class Monitor
      include Enumerable

      def initialize(socket)
        @socket = socket
      end

      def recv(timeout: nil)
        @socket.monitor_event(timeout: timeout)
      end

      def recv_nowait
        @socket.try_monitor_event
      end

      def each
        return enum_for(__method__) unless block_given?

        loop { yield recv }
      rescue IOError
        raise unless @socket.closed?
      end
    end

    class Socket
      attr_reader :socket_type

      def initialize(recv_timeout: nil, send_timeout: nil, curve_auth: nil, **options)
        socket_type = self.class.const_get(:SOCKET_TYPE, false)
        @socket_type = socket_type.to_s.downcase.to_sym
        unless SOCKET_TYPES.include?(@socket_type)
          raise ArgumentError, "unknown socket type: #{socket_type}"
        end

        @recv_timeout = recv_timeout
        @send_timeout = send_timeout
        @recv_batch   = []
        @request_waiting = false
        @reply_ready     = false
        @native       = Native::Socket.new(@socket_type.to_s.upcase)
        @native.set_options(normalize_options(options))
        @materialize_lock = Mutex.new
        @materialized = false
        @recv_io = nil
        @send_io = nil
        set_curve_auth(curve_auth) unless curve_auth.nil?
      end

      def bind(endpoint)
        ensure_materialized
        @native.bind(String(endpoint))
      end

      def connect(endpoint)
        ensure_materialized
        @native.connect(String(endpoint))
        self
      end

      def disconnect(endpoint)
        ensure_materialized
        @native.disconnect(String(endpoint))
        self
      end

      def unbind(endpoint)
        ensure_materialized
        @native.unbind(String(endpoint))
        self
      end

      def peer_info(routing_id)
        ensure_materialized
        @native.peer_info(routing_id)
      end

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
      alias << send

      def try_send(message, *more)
        ensure_materialized
        parts = normalize_parts(message, more)
        validate_send_parts!(parts)
        validate_pattern_state_before_send!
        return false unless enqueue(parts) == :ok

        sent!
        true
      end

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
      alias receive recv

      def try_recv
        ensure_materialized
        unless @recv_batch.empty?
          message = @recv_batch.shift
          received!
          return message
        end

        message = if ROUTED_TYPES.include?(@socket_type)
          @native.try_recv_routed
        elsif (batch = @native.try_recv_batch)
          first = batch.shift
          @recv_batch = batch
          first
        end
        received! if message
        message
      end

      def each
        return enum_for(__method__) unless block_given?

        loop { yield recv }
      rescue IOError
        raise unless closed?
      end

      def subscribe(prefix = "")
        ensure_materialized
        @native.subscribe(String(prefix).b)
        self
      end

      def unsubscribe(prefix = "")
        ensure_materialized
        @native.unsubscribe(String(prefix).b)
        self
      end

      def join(group)
        ensure_materialized
        @native.join(String(group).b)
        self
      end

      def leave(group)
        ensure_materialized
        @native.leave(String(group).b)
        self
      end

      def publish(group, message)
        send(group, message)
      end

      def wait_for_peer(timeout: nil)
        ensure_materialized
        wait_for_native_fd(@native.peer_connected_fd, timeout, "peer connection timed out")
        self
      end

      def wait_for_subscriber(timeout: nil)
        ensure_materialized
        wait_for_native_fd(@native.subscriber_joined_fd, timeout, "subscriber timed out")
        self
      end

      def monitor
        ensure_materialized
        @monitor ||= Monitor.new(self)
      end

      def monitor_event(timeout: @recv_timeout)
        ensure_materialized
        event = @native.try_recv_monitor
        return event if event

        wait_for_native_fd(@native.monitor_fd, timeout, "monitor receive timed out")
        @native.try_recv_monitor
      end

      def try_monitor_event
        ensure_materialized
        @native.try_recv_monitor
      end

      def close
        return if closed?

        @native.close
        close_wrapper(@recv_io)
        close_wrapper(@send_io)
        nil
      end

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
        options.to_h do |key, value|
          value = value.to_s if value.is_a?(Symbol)
          value = value.transform_keys(&:to_s) if value.is_a?(Hash)
          [key.to_s, value]
        end
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

    SOCKET_TYPES.each do |type|
      klass = Class.new(Socket)
      klass.const_set(:SOCKET_TYPE, type)
      const_set(type.to_s.upcase, klass)
    end
  end
end
