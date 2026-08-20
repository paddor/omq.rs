# frozen_string_literal: true

require_relative "rs/version"
require_relative "rs/omq_rs_native"
require_relative "rs/socket"

module OMQ
  class << self
    # Returns the OMQ.rs namespace or creates an OMQ.rs-backed socket.
    #
    # @param socket_type [Symbol, String, nil] socket pattern, such as +:push+
    # @param options [Hash] socket options passed to {Rust.socket}
    # @yield [socket] yields a newly created socket and closes it afterward
    # @yieldparam socket [Rust::Socket]
    # @return [Module, Rust::Socket, Object] the namespace, socket, or block result
    # @raise [ArgumentError] if +socket_type+ is unknown or an option is invalid
    def rs(socket_type = nil, **options)
      return Rust if socket_type.nil?

      socket = Rust.socket(socket_type, **options)
      return socket unless block_given?

      begin
        yield socket
      ensure
        socket.close
      end
    end
  end
end
