# frozen_string_literal: true

require_relative "rs/version"
require_relative "rs/omq_rs_native"
require_relative "rs/socket"

module OMQ
  class << self
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
