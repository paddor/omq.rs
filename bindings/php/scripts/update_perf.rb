#!/usr/bin/env ruby
# frozen_string_literal: true

# Measure the PECL zmq extension on libzmq vs on libomq_zmq over 2-process
# TCP loopback. Rows are appended to ~/.cache/omq.php/bindings.jsonl; the
# chart uses the latest row per implementation, pattern, and size.
#
# Env: OMQ_PHP (PHP command with the zmq extension, default "php").

require "cgi"
require "fileutils"
require "json"
require "open3"
require "optparse"
require "shellwords"
require "time"
require "timeout"

ROOT = File.expand_path("..", __dir__)
REPO = File.expand_path("../..", ROOT)
PEER = File.join(__dir__, "bench_peer.php")
CACHE = File.join(ENV.fetch("XDG_CACHE_HOME", File.join(Dir.home, ".cache")), "omq.php")
RESULTS = File.join(CACHE, "bindings.jsonl")
CHART = File.join(ROOT, "doc", "charts", "bindings.svg")
COMPAT = File.join(REPO, "target", "omq-libzmq-compat", "release")
PHP = Shellwords.split(ENV.fetch("OMQ_PHP", "php"))
SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16_384, 32_768].freeze
QUICK_SIZES = [16, 1024, 32_768].freeze
LATENCY_MAX_SIZE = 4096
IMPLS = %w[omq libzmq].freeze
LABELS = {"omq" => "ext-zmq on OMQ (libomq_zmq)", "libzmq" => "ext-zmq on libzmq"}.freeze
COLORS = {"omq" => "#ef4444", "libzmq" => "#60a5fa"}.freeze
DRAW_ORDER = %w[libzmq omq].freeze

def parse_options
  options = {quick: false, chart_only: false, record: true, impls: IMPLS, patterns: %w[pushpull reqrep],
             rounds: 3, sizes: nil, duration: 2.0, warmup: 0.5, latency_duration: 1.5}
  OptionParser.new do |parser|
    parser.on("--quick", "1 round, 3 sizes, short runs, no writes") { options[:quick] = true }
    parser.on("--chart-only") { options[:chart_only] = true }
    parser.on("--no-record", "measure only, no writes") { options[:record] = false }
    parser.on("--impl NAMES") { |value| options[:impls] = value.split(",") }
    parser.on("--patterns NAMES") { |value| options[:patterns] = value.split(",") }
    parser.on("--rounds N", Integer) { |value| options[:rounds] = value }
    parser.on("--sizes BYTES") { |value| options[:sizes] = value.split(",").map { |size| Integer(size) } }
  end.parse!
  options.merge!(rounds: 1, duration: 0.5, warmup: 0.1, latency_duration: 0.5, record: false) if options[:quick]
  options[:sizes] ||= options[:quick] ? QUICK_SIZES : SIZES
  options
end

def stage_omq
  system(File.join(REPO, "omq-libzmq", "scripts", "stage-compat.sh"), "--release", COMPAT,
         out: File::NULL, exception: true)
end

def peer_env(impl)
  return {"LD_LIBRARY_PATH" => nil, "DYLD_LIBRARY_PATH" => nil} if impl == "libzmq"

  lib = File.join(COMPAT, "lib")
  {"LD_LIBRARY_PATH" => lib, "DYLD_LIBRARY_PATH" => lib}
end

def spawn_peer(impl, args, streams)
  stdin, stdout, stderr, wait = Open3.popen3(peer_env(impl), *PHP, PEER, impl, *args.map(&:to_s))
  stdin.close
  streams.concat([stdout, stderr])
  [stdout, stderr, wait]
end

def read_line(io, timeout)
  Timeout.timeout(timeout) { io.gets }
end

def stop(wait)
  return unless wait&.alive?

  Process.kill("TERM", wait.pid)
  wait.join(2)
  Process.kill("KILL", wait.pid) if wait.alive?
rescue Errno::ESRCH, Errno::ECHILD
end

def run_cell(impl, pattern, size, options)
  server_role, client_role = pattern == "pushpull" ? %w[pull push] : %w[rep req]
  duration = pattern == "pushpull" ? options[:duration] : options[:latency_duration]
  timing = [size, duration, options[:warmup]]
  streams = []
  server_out, server_err, server = spawn_peer(impl, [pattern, server_role, "tcp://127.0.0.1:*", *timing], streams)
  endpoint = read_line(server_out, 10)&.[](/\AENDPOINT (\S+)/, 1)
  raise "#{impl} #{pattern}: no endpoint: #{server_err.read}" unless endpoint

  client_out, client_err, client = spawn_peer(impl, [pattern, client_role, endpoint, *timing], streams)
  out, err = pattern == "pushpull" ? [server_out, server_err] : [client_out, client_err]
  line = read_line(out, duration + options[:warmup] + 20)
  raise "#{impl} #{pattern} #{size}: no result: #{err.read}" unless line&.start_with?("RESULT ")

  JSON.parse(line.delete_prefix("RESULT "), symbolize_names: true)
ensure
  stop(client)
  stop(server)
  streams.each { |stream| stream.close unless stream.closed? }
end

def php_version
  Open3.capture2(*PHP, "-r", "echo PHP_VERSION;").first
end

def measure(options)
  run_id = Time.now.utc.iso8601
  php = php_version
  options[:impls].each do |impl|
    options[:patterns].each do |pattern|
      sizes = pattern == "reqrep" ? options[:sizes].select { |size| size <= LATENCY_MAX_SIZE } : options[:sizes]
      sizes.each do |size|
        key = pattern == "pushpull" ? :msgs_s : :p50_us
        runs = Array.new(options[:rounds]) { run_cell(impl, pattern, size, options) }
        row = runs.sort_by { |run| run.fetch(key) }.fetch(runs.length / 2)
        row = row.merge(run_id: run_id, php: php, rounds: options[:rounds])
        append(row) if options[:record]
        metric = pattern == "pushpull" ? "#{fmt_rate(row[:msgs_s])} msg/s" : format("p50 %.1f us", row[:p50_us])
        puts format("%-7s %-8s %6d B  %s", impl, pattern, size, metric)
      end
    end
  end
end

def append(row)
  FileUtils.mkdir_p(CACHE)
  File.open(RESULTS, "a") { |file| file.puts(JSON.generate(row)) }
end

def latest_results
  return {} unless File.exist?(RESULTS)

  File.foreach(RESULTS).each_with_object({}) do |line, rows|
    row = JSON.parse(line, symbolize_names: true)
    rows[[row[:impl], row[:pattern], row[:size]]] = row
  rescue JSON::ParserError
    next
  end
end

def fmt_size(size)
  size >= 1024 ? "#{size / 1024} KiB" : "#{size} B"
end

def fmt_rate(value)
  return format("%gM", (value / 1e6).round(2)) if value >= 1e6
  return format("%gk", (value / 1e3).round(1)) if value >= 1e3

  format("%g", value)
end

def nice_ceil(value)
  return 1 if value <= 0

  base = 10**Math.log10(value).floor
  [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10].map { |multiple| multiple * base }.find { |candidate| candidate >= value }
end

def hardware_label
  path = File.join(REPO, ".chart_hw")
  config = {}
  if File.file?(path)
    File.foreach(path) do |line|
      key, sep, value = line.strip.partition("=")
      config[key.strip] = value.strip if sep == "=" && !key.start_with?("#")
    end
  end
  label = [ENV["OMQ_HW_PREFIX"] || config["prefix"], ENV["OMQ_HW_POSTFIX"] || config["postfix"]].compact.join(", ")
  label unless label.empty?
end

# Chart geometry. Throughput: small sizes (msg/s) left, large sizes (GB/s)
# right. Latency: REQ/REP p50 across the full width.
Panel = Struct.new(:left, :right, :top, :bottom) do
  def x(index, count) = left + index * (right - left).to_f / [count - 1, 1].max
  def y(value, max) = bottom - value / max * (bottom - top)
end

def text(x, y, body, size: 10, anchor: "middle", fill: "#e5e7eb", bold: false, extra: "")
  weight = bold ? %( font-weight="700") : ""
  %(  <text x="#{x.round(1)}" y="#{y.round(1)}" text-anchor="#{anchor}" fill="#{fill}" font-size="#{size}"#{weight}#{extra}>#{body}</text>)
end

def line(x1, y1, x2, y2, stroke: "#374151", width: 1)
  %(  <line x1="#{x1.round(1)}" y1="#{y1.round(1)}" x2="#{x2.round(1)}" y2="#{y2.round(1)}" stroke="#{stroke}" stroke-width="#{width}"/>)
end

def series(points, color, dashed: false)
  encoded = points.map { |x, y| "#{x.round(1)},#{y.round(1)}" }.join(" ")
  style = dashed ? %(stroke-width="2" stroke-dasharray="6,4") : %(stroke-width="2.5" stroke-linejoin="round")
  out = [%(  <polyline points="#{encoded}" fill="none" stroke="#{color}" #{style}/>)]
  return out if dashed

  out + points.map { |x, y| %(  <circle cx="#{x.round(1)}" cy="#{y.round(1)}" r="3" fill="#{color}" stroke="#000000"/>) }
end

def grid(panel, sizes, ticks, max, label_x, anchor, &format_tick)
  out = ticks.flat_map do |tick|
    y = panel.y(tick, max)
    [line(panel.left, y, panel.right, y), text(label_x, y, format_tick.call(tick), anchor: anchor, extra: %( dominant-baseline="middle"))]
  end
  sizes.each_index do |index|
    x = panel.x(index, sizes.length)
    out << line(x, panel.top, x, panel.bottom)
    out << text(x, panel.bottom + 14, fmt_size(sizes[index]), size: 8.5)
  end
  out << line(panel.left, panel.bottom, panel.right, panel.bottom, stroke: "#9ca3af", width: 1.5)
end

def throughput_lines(records, top)
  rows = records.values.select { |row| row[:pattern] == "pushpull" }
  small_sizes = rows.map { |row| row[:size] }.uniq.sort.select { |size| size <= 1024 }
  large_sizes = rows.map { |row| row[:size] }.uniq.sort.select { |size| size >= 256 }
  small = Panel.new(60, 395, top, top + 335)
  large = Panel.new(455, 790, top, top + 335)
  max_rate = nice_ceil(rows.select { |row| row[:size] <= 1024 }.map { |row| row[:msgs_s] }.max || 0)
  max_gbps = [1, rows.select { |row| row[:size] >= 256 }.map { |row| row[:msgs_s] * row[:size] / 1e9 }.max.to_f.ceil].max
  out = [text(227.5, top - 17, "small messages", size: 12, fill: "#f9fafb", bold: true),
         text(622.5, top - 17, "medium/large messages", size: 12, fill: "#f9fafb", bold: true)]
  out += grid(small, small_sizes, (1..10).map { |i| max_rate * i / 10.0 }, max_rate, 52, "end") { |v| fmt_rate(v) }
  out += grid(large, large_sizes, (1..(max_gbps * 2)).map { |i| i / 2.0 }, max_gbps, 798, "start") { |v| format("%g GB/s", v) }
  out << line(small.left, top, small.left, small.bottom, stroke: "#9ca3af", width: 1.5)
  out << line(large.right, top, large.right, large.bottom, stroke: "#9ca3af", width: 1.5)
  DRAW_ORDER.each do |impl|
    rate = small_sizes.each_with_index.filter_map do |size, i|
      row = records[[impl, "pushpull", size]]
      [small.x(i, small_sizes.length), small.y(row[:msgs_s], max_rate)] if row
    end
    gbps = large_sizes.each_with_index.filter_map do |size, i|
      row = records[[impl, "pushpull", size]]
      [large.x(i, large_sizes.length), large.y(row[:msgs_s] * size / 1e9, max_gbps)] if row
    end
    out += series(rate, COLORS[impl], dashed: true) + series(gbps, COLORS[impl])
  end
  out << text(425, small.bottom + 32, "dashed = message rate &#183; solid = bandwidth", size: 9, fill: "#9ca3af")
end

def latency_lines(records, top)
  rows = records.values.select { |row| row[:pattern] == "reqrep" && row[:size] <= LATENCY_MAX_SIZE }
  sizes = rows.map { |row| row[:size] }.uniq.sort
  panel = Panel.new(60, 790, top, top + 230)
  max = [100, ((rows.map { |row| row[:p50_us] }.max.to_f * 1.15) / 20.0).ceil * 20].max
  out = [text(425, top - 17, "REQ/REP latency: 2-process, TCP loopback, p50 &#956;s (lower is better)",
              size: 13, fill: "#f9fafb", bold: true)]
  out += grid(panel, sizes, (20..max).step(20).to_a, max.to_f, 52, "end") { |v| "#{v} &#956;s" }
  out << line(panel.left, top, panel.left, panel.bottom, stroke: "#9ca3af", width: 1.5)
  DRAW_ORDER.each do |impl|
    points = sizes.each_with_index.filter_map do |size, i|
      row = records[[impl, "reqrep", size]]
      [panel.x(i, sizes.length), panel.y(row[:p50_us], max.to_f)] if row
    end
    out += series(points, COLORS[impl])
  end
  out
end

def legend_lines(y)
  width = 260
  start = 425 - IMPLS.length * width / 2.0
  IMPLS.each_with_index.flat_map do |impl, index|
    x = start + index * width
    [line(x, y, x + 14, y, stroke: COLORS[impl], width: 2.5),
     %(  <circle cx="#{(x + 7).round(1)}" cy="#{y}" r="2.5" fill="#{COLORS[impl]}"/>),
     text(x + 20, y + 4, CGI.escapeHTML(LABELS[impl]), size: 11, anchor: "start")]
  end
end

def render_chart
  records = latest_results
  return warn("no benchmark results: #{RESULTS}") if records.empty?

  hw = hardware_label
  offset = hw ? 14 : 0
  top = 95 + offset
  latency_top = top + 440
  height = latency_top + 300
  out = [%(<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 850 #{height}" font-family="system-ui, -apple-system, sans-serif">),
         %(  <rect width="850" height="#{height}" fill="#000000"/>),
         text(425, top - 65, "PUSH/PULL throughput: 2-process, TCP loopback (higher is better)",
              size: 13, fill: "#f9fafb", bold: true)]
  out << text(425, top - 51, CGI.escapeHTML(hw), fill: "#9ca3af") if hw
  out += throughput_lines(records, top) + latency_lines(records, latency_top) + legend_lines(latency_top + 270)
  FileUtils.mkdir_p(File.dirname(CHART))
  File.write(CHART, (out + ["</svg>"]).join("\n") + "\n")
  puts "chart: #{CHART}"
end

options = parse_options
unless options[:chart_only]
  stage_omq if options[:impls].include?("omq")
  measure(options)
end
render_chart if options[:record]
