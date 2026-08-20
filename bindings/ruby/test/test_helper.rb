# frozen_string_literal: true

require "minitest/autorun"
require "timeout"
require "omq/rs"

Warning[:experimental] = false

module SocketTestHelpers
  def socket(type, **options)
    value = OMQ.rs(type, linger: 0, **options)
    (@sockets ||= []) << value
    value
  end

  def teardown
    @sockets&.reverse_each(&:close)
  end

  def tcp_endpoint(bound)
    bound.bind("tcp://127.0.0.1:0")
  end
end

class Minitest::Test
  include SocketTestHelpers
end
