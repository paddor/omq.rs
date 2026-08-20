# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("omq/rs/omq_rs_native") do |r|
  r.profile = ENV.fetch("RB_SYS_CARGO_PROFILE", :release).to_sym
end
