#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "cgi"
require "json"
require "open3"
require "optparse"
require "rbconfig"
require "timeout"
require "time"

ROOT       = File.expand_path("..", __dir__)
REPO_ROOT  = File.expand_path("../..", ROOT)
PEER       = File.join(__dir__, "bench_peer.rb")
CACHE      = File.join(ENV.fetch("XDG_CACHE_HOME", File.join(Dir.home, ".cache")), "omq-rs")
RESULTS    = File.join(CACHE, "bindings.jsonl")
CHART      = File.join(ROOT, "doc", "charts", "bindings.svg")
SIZES      = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16_384, 32_768].freeze
QUICK_SIZE = [128, 1024].freeze
LATENCY_MAX_SIZE = 4096
LATENCY_MIN_US = 0.0
LATENCY_MAX_US = 180.0
LATENCY_STEP_US = 20
COLORS     = {"omq-rs" => "#ef4444", "cztop" => "#60a5fa", "ffi-rzmq" => "#a855f7"}.freeze
DRAW_ORDER = %w[cztop ffi-rzmq omq-rs].freeze
LABELS     = {
  "omq-rs" => "omq-rs (OMQ.rs)",
  "cztop" => "cztop (CZMQ/libzmq)",
  "ffi-rzmq" => "ffi-rzmq (libzmq)",
}.freeze

def parse_options
  options = {
    quick: false,
    chart_only: false,
    record: true,
    implementations: %w[omq-rs cztop ffi-rzmq],
    patterns: %w[pushpull reqrep],
    rounds: 3,
    sizes: nil,
  }
  OptionParser.new do |parser|
    parser.on("--quick") { options[:quick] = true }
    parser.on("--chart-only") { options[:chart_only] = true }
    parser.on("--no-record") { options[:record] = false }
    parser.on("--impl NAMES") { |value| options[:implementations] = value.split(",") }
    parser.on("--patterns NAMES") { |value| options[:patterns] = value.split(",") }
    parser.on("--rounds N", Integer) { |value| options[:rounds] = value }
    parser.on("--sizes BYTES") { |value| options[:sizes] = value.split(",").map { |size| Integer(size) } }
  end.parse!
  options[:rounds] = 1 if options[:quick]
  options
end

def peer_command(*args)
  [RbConfig.ruby, "--yjit", "-I#{File.join(ROOT, "lib")}", PEER, *args.map(&:to_s)]
end

def implementation_available?(name)
  library = {"omq-rs" => "omq/rs", "cztop" => "cztop", "ffi-rzmq" => "ffi-rzmq"}.fetch(name)
  system(RbConfig.ruby, "-I#{File.join(ROOT, "lib")}", "-e", "require #{library.dump}",
         out: File::NULL, err: File::NULL)
end

def counts(pattern, size, quick)
  if pattern == "pushpull"
    target_bytes = quick ? 16 * 1024 * 1024 : 256 * 1024 * 1024
    count = [target_bytes / size, quick ? 20_000 : 100_000].max
  else
    count = quick ? 10_000 : 100_000
  end
  [count, [count / 10, 10_000].min]
end

def read_line(io, timeout: 10)
  Timeout.timeout(timeout) { io.gets }
end

def stop_process(wait_thread)
  return unless wait_thread&.alive?

  Process.kill("TERM", wait_thread.pid)
  wait_thread.join(2)
  Process.kill("KILL", wait_thread.pid) if wait_thread.alive?
rescue Errno::ESRCH, Errno::ECHILD
end

def run_cell(backend, pattern, size, quick)
  count, warmup = counts(pattern, size, quick)
  server_role, client_role = pattern == "pushpull" ? %w[pull push] : %w[rep req]
  server = nil

  Timeout.timeout(60) do
    Open3.popen3(*peer_command(backend, pattern, server_role, "tcp://127.0.0.1:0", size, count, warmup)) do |_stdin, stdout, stderr, wait|
      server = wait
      endpoint_line = read_line(stdout)
      endpoint = endpoint_line&.match(/\AENDPOINT (.+)\n?\z/)&.[](1)
      raise "server failed: #{stderr.read}" unless endpoint

      client_out, client_err, client_status = Open3.capture3(
        *peer_command(backend, pattern, client_role, endpoint, size, count, warmup),
      )
      raise "client failed: #{client_err}" unless client_status.success?

      measured_output = pattern == "pushpull" ? read_line(stdout, timeout: 30) : client_out.lines.find { |line| line.start_with?("RESULT ") }
      result_line = measured_output&.sub(/\ARESULT /, "")
      raise "missing result for #{backend} #{pattern} #{size}" unless result_line

      status = wait.value
      raise "server failed: #{stderr.read}" unless status.success?

      JSON.parse(result_line, symbolize_names: true)
    end
  end
ensure
  stop_process(server)
end

def append_result(result)
  FileUtils.mkdir_p(CACHE)
  record = result.merge(
    timestamp: Time.now.utc.iso8601,
    ruby: RUBY_DESCRIPTION,
  )
  File.open(RESULTS, "a") { |file| file.puts(JSON.generate(record)) }
end

def latest_results
  return {} unless File.exist?(RESULTS)

  File.foreach(RESULTS).each_with_object({}) do |line, records|
    record = JSON.parse(line, symbolize_names: true)
    records[[record[:backend], record[:pattern], record[:size]]] = record
  rescue JSON::ParserError
  end
end

def fmt_size(size)
  size >= 1024 ? "#{size / 1024} KiB" : "#{size} B"
end

def nice_ceil(value)
  return 1 if value <= 0

  base = 10**Math.log10(value).floor
  [1, 2, 5, 10].map { |multiple| multiple * base }.find { |candidate| candidate >= value }
end

def fmt_rate(value)
  return format("%gM", value / 1_000_000.0) if value >= 1_000_000
  return format("%gk", value / 1_000.0) if value >= 1_000

  format("%g", value)
end

def hardware_label
  config = {}
  path = File.join(REPO_ROOT, ".chart_hw")
  if File.file?(path)
    File.foreach(path) do |line|
      line = line.strip
      next if line.empty? || line.start_with?("#")

      key, separator, value = line.partition("=")
      config[key.strip] = value.strip if separator == "="
    end
  end

  label = ENV["OMQ_HW_LABEL"] || config["label"]
  return label if label

  prefix = ENV["OMQ_HW_PREFIX"] || config["prefix"]
  postfix = ENV["OMQ_HW_POSTFIX"] || config["postfix"]
  combined = [prefix, postfix].compact.join(", ")
  combined unless combined.empty?
end

def render_chart
  records = latest_results
  return warn("no benchmark results: #{RESULTS}") if records.empty?

  sizes = records.values.filter_map { |record| record[:size] if record[:pattern] == "pushpull" }.uniq.sort
  latency_sizes = records.values.filter_map do |record|
    record[:size] if record[:pattern] == "reqrep" && record[:size] <= LATENCY_MAX_SIZE
  end.uniq.sort
  hw = hardware_label
  hw_offset = hw ? 14 : 0
  svg_width = 850
  svg_height = 810 + hw_offset
  left = 60
  right = 790
  middle = (left + right) / 2.0
  small_left = 60
  small_right = 395
  large_left = 455
  large_right = 790
  throughput_top = 95 + hw_offset
  throughput_bottom = 430 + hw_offset
  latency_top = throughput_bottom + 105
  latency_bottom = latency_top + 200
  legend_y = latency_bottom + 40
  footer_y = legend_y + 18
  small_sizes = sizes.select { |size| size <= 1024 }
  large_sizes = sizes.select { |size| size >= 256 }
  x_small = ->(index) { small_left + index * (small_right - small_left).to_f / [small_sizes.length - 1, 1].max }
  x_large = ->(index) { large_left + index * (large_right - large_left).to_f / [large_sizes.length - 1, 1].max }
  x_latency = ->(index) { left + index * (right - left).to_f / [latency_sizes.length - 1, 1].max }

  throughput = records.values.filter { |record| record[:pattern] == "pushpull" }
  small_rates = throughput.filter_map do |record|
    record[:messages_per_second] if record[:size] <= 1024
  end
  bandwidths = throughput.filter_map do |record|
    record[:messages_per_second] * record[:size] / 1_000_000_000.0 if record[:size] >= 256
  end
  max_rate = nice_ceil(small_rates.max || 0)
  max_gbps = [1, (bandwidths.max || 0).ceil].max
  latency_span = LATENCY_MAX_US - LATENCY_MIN_US
  y_rate = ->(value) { throughput_bottom - value / max_rate * (throughput_bottom - throughput_top) }
  y_gbps = ->(value) { throughput_bottom - value / max_gbps * (throughput_bottom - throughput_top) }
  y_latency = lambda do |value|
    fraction = (value - LATENCY_MIN_US) / latency_span
    latency_bottom - fraction * (latency_bottom - latency_top)
  end

  FileUtils.mkdir_p(File.dirname(CHART))
  lines = [
    %(<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 #{svg_width} #{svg_height}" font-family="system-ui, -apple-system, sans-serif">),
    %(  <rect width="#{svg_width}" height="#{svg_height}" fill="#000000"/>),
    %(  <text x="#{middle}" y="#{throughput_top - 65}" text-anchor="middle" fill="#f9fafb" font-size="13" font-weight="700">PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>),
  ]
  lines << %(  <text x="#{middle}" y="#{throughput_top - 51}" text-anchor="middle" fill="#9ca3af" font-size="10">#{CGI.escapeHTML(hw)}</text>) if hw

  10.times do |index|
    fraction = (index + 1) / 10.0
    y = throughput_bottom - fraction * (throughput_bottom - throughput_top)
    lines << %(  <line x1="#{small_left}" y1="#{y.round(1)}" x2="#{small_right}" y2="#{y.round(1)}" stroke="#374151" stroke-width="1"/>)
    lines << %(  <text x="#{small_left - 8}" y="#{y.round(1)}" text-anchor="end" dominant-baseline="middle" fill="#e5e7eb" font-size="10">#{fmt_rate(max_rate * fraction)}</text>)
  end
  (1..(max_gbps * 2)).each do |index|
    value = index / 2.0
    y = y_gbps.call(value)
    lines << %(  <line x1="#{large_left}" y1="#{y.round(1)}" x2="#{large_right}" y2="#{y.round(1)}" stroke="#374151" stroke-width="1"/>)
    lines << %(  <text x="#{large_right + 8}" y="#{y.round(1)}" text-anchor="start" dominant-baseline="middle" fill="#e5e7eb" font-size="10">#{format('%g GB/s', value)}</text>)
  end
  small_sizes.each_with_index do |_size, index|
    x = x_small.call(index)
    lines << %(  <line x1="#{x.round(1)}" y1="#{throughput_top}" x2="#{x.round(1)}" y2="#{throughput_bottom}" stroke="#374151" stroke-width="1"/>)
  end
  large_sizes.each_with_index do |_size, index|
    x = x_large.call(index)
    lines << %(  <line x1="#{x.round(1)}" y1="#{throughput_top}" x2="#{x.round(1)}" y2="#{throughput_bottom}" stroke="#374151" stroke-width="1"/>)
  end
  lines << %(  <line x1="#{small_left}" y1="#{throughput_top}" x2="#{small_left}" y2="#{throughput_bottom}" stroke="#9ca3af" stroke-width="1.5"/>)
  lines << %(  <line x1="#{small_left}" y1="#{throughput_bottom}" x2="#{small_right}" y2="#{throughput_bottom}" stroke="#9ca3af" stroke-width="1.5"/>)
  lines << %(  <line x1="#{large_left}" y1="#{throughput_bottom}" x2="#{large_right}" y2="#{throughput_bottom}" stroke="#9ca3af" stroke-width="1.5"/>)
  lines << %(  <line x1="#{large_right}" y1="#{throughput_top}" x2="#{large_right}" y2="#{throughput_bottom}" stroke="#9ca3af" stroke-width="1.5"/>)
  lines << %(  <text x="#{(small_left + small_right) / 2.0}" y="#{throughput_top - 17}" text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">small messages</text>)
  lines << %(  <text x="#{(large_left + large_right) / 2.0}" y="#{throughput_top - 17}" text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">medium/large messages</text>)

  DRAW_ORDER.each do |backend|
    color = COLORS.fetch(backend)
    rate_points = []
    bandwidth_points = []
    small_sizes.each_with_index do |size, index|
      record = records[[backend, "pushpull", size]]
      next unless record

      rate_points << [x_small.call(index), y_rate.call(record[:messages_per_second])]
    end
    large_sizes.each_with_index do |size, index|
      record = records[[backend, "pushpull", size]]
      next unless record

      gbps = record[:messages_per_second] * size / 1_000_000_000.0
      bandwidth_points << [x_large.call(index), y_gbps.call(gbps)]
    end
    lines << "  #{polyline(rate_points, color, dashed: true)}"
    lines << "  #{polyline(bandwidth_points, color)}"
    bandwidth_points.each do |x, y|
      lines << %(  <circle cx="#{x.round(1)}" cy="#{y.round(1)}" r="3" fill="#{color}" stroke="#000000" stroke-width="1"/>)
    end
  end
  small_sizes.each_with_index do |size, index|
    lines << %(  <text x="#{x_small.call(index).round(1)}" y="#{throughput_bottom + 14}" text-anchor="middle" fill="#e5e7eb" font-size="8.5">#{fmt_size(size)}</text>)
  end
  large_sizes.each_with_index do |size, index|
    lines << %(  <text x="#{x_large.call(index).round(1)}" y="#{throughput_bottom + 14}" text-anchor="middle" fill="#e5e7eb" font-size="8.5">#{fmt_size(size)}</text>)
  end
  lines << %(  <text x="#{middle}" y="#{throughput_bottom + 32}" text-anchor="middle" fill="#9ca3af" font-size="9">dashed = message rate · solid = bandwidth</text>)

  lines << %(  <text x="#{middle}" y="#{latency_top - 17}" text-anchor="middle" fill="#f9fafb" font-size="13" font-weight="700">REQ/REP mean latency: 2-process, TCP loopback (lower is better)</text>)
  (LATENCY_MIN_US.to_i..LATENCY_MAX_US.to_i).step(LATENCY_STEP_US) do |value|
    fraction = (value - LATENCY_MIN_US) / latency_span
    y = latency_bottom - fraction * (latency_bottom - latency_top)
    lines << %(  <line x1="#{left}" y1="#{y.round(1)}" x2="#{right}" y2="#{y.round(1)}" stroke="#374151" stroke-width="1"/>)
    lines << %(  <text x="#{left - 8}" y="#{y.round(1)}" text-anchor="end" dominant-baseline="middle" fill="#e5e7eb" font-size="10">#{value} μs</text>)
  end
  latency_sizes.each_with_index do |size, index|
    x = x_latency.call(index)
    lines << %(  <line x1="#{x.round(1)}" y1="#{latency_top}" x2="#{x.round(1)}" y2="#{latency_bottom}" stroke="#374151" stroke-width="1"/>)
  end
  lines << %(  <line x1="#{left}" y1="#{latency_top}" x2="#{left}" y2="#{latency_bottom}" stroke="#9ca3af" stroke-width="1.5"/>)
  lines << %(  <line x1="#{left}" y1="#{latency_bottom}" x2="#{right}" y2="#{latency_bottom}" stroke="#9ca3af" stroke-width="1.5"/>)
  DRAW_ORDER.each do |backend|
    color = COLORS.fetch(backend)
    points = latency_sizes.each_with_index.filter_map do |size, index|
      record = records[[backend, "reqrep", size]]
      [x_latency.call(index), y_latency.call(record[:microseconds_per_round_trip])] if record
    end
    lines << "  #{polyline(points, color)}"
    points.each do |x, y|
      lines << %(  <circle cx="#{x.round(1)}" cy="#{y.round(1)}" r="3" fill="#{color}" stroke="#000000" stroke-width="1"/>)
    end
  end
  latency_sizes.each_with_index do |size, index|
    lines << %(  <text x="#{x_latency.call(index).round(1)}" y="#{latency_bottom + 14}" text-anchor="middle" fill="#e5e7eb" font-size="8.5">#{fmt_size(size)}</text>)
  end

  item_width = 250
  legend_start = middle - COLORS.length * item_width / 2.0
  COLORS.each_with_index do |(backend, color), index|
    x = legend_start + index * item_width
    lines << %(  <line x1="#{x.round}" y1="#{legend_y}" x2="#{(x + 14).round}" y2="#{legend_y}" stroke="#{color}" stroke-width="2.5"/>)
    lines << %(  <circle cx="#{(x + 7).round}" cy="#{legend_y}" r="2.5" fill="#{color}"/>)
    lines << %(  <text x="#{(x + 20).round}" y="#{legend_y + 4}" fill="#e5e7eb" font-size="11" font-weight="500">#{LABELS.fetch(backend)}</text>)
  end

  lines << %(  <text x="#{middle}" y="#{footer_y}" text-anchor="middle" fill="#9ca3af" font-size="9">solid = mean latency</text>)
  lines << "</svg>"
  File.write(CHART, lines.join("\n") + "\n")
  puts "chart: #{CHART}"
end

def polyline(points, color, dashed: false)
  encoded = points.map { |x, y| "#{x.round(1)},#{y.round(1)}" }.join(" ")
  style = if dashed
            %(stroke-width="2" stroke-dasharray="6,4")
          else
            %(stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round")
          end
  %(<polyline points="#{encoded}" fill="none" stroke="#{color}" #{style}/>)
end

options = parse_options
unless options[:chart_only]
  sizes = options[:sizes] || (options[:quick] ? QUICK_SIZE : SIZES)
  implementations = options[:implementations].select do |implementation|
    available = implementation_available?(implementation)
    warn "skip: #{implementation} unavailable" unless available
    available
  end

  implementations.each do |implementation|
    options[:patterns].each do |pattern|
      pattern_sizes = pattern == "reqrep" ? sizes.select { |size| size <= LATENCY_MAX_SIZE } : sizes
      pattern_sizes.each do |size|
        best = options[:rounds].times.map { run_cell(implementation, pattern, size, options[:quick]) }
          .min_by { |result| pattern == "pushpull" ? -result[:messages_per_second] : result[:microseconds_per_round_trip] }
        append_result(best.merge(rounds: options[:rounds])) if options[:record] && !options[:quick]
        metric = pattern == "pushpull" ? "#{best[:messages_per_second].round} msg/s" : format("%.1f μs", best[:microseconds_per_round_trip])
        puts "#{implementation.ljust(8)} #{pattern.ljust(8)} #{size.to_s.rjust(6)} B  #{metric}"
      end
    end
  end
end

render_chart if options[:record] && !options[:quick]
