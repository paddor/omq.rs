#!/usr/bin/env ruby
# One campaign at a time. Every subscriber must verify every publication.
require 'digest'
require 'fileutils'
require 'json'
require 'open3'
require 'optparse'
require 'socket'
require 'tmpdir'
require 'time'

SIZES = [16, 64, 256, 1024, 4096, 16_384, 131_072, 1_048_576, 8_388_608].freeze
IMPLEMENTATIONS = %w[inproc ipc shm shm-pool].freeze
HWM = 1000
SLAB_BYTES = 4096
BATCH_MESSAGES = 256
BATCH_BYTES = 1024 * 1024
POOL_BYTES = 64 * 1024 * 1024
LARGE_POOL_BYTES = 256 * 1024 * 1024
LARGE_POOL_MIN_BODY = 1024 * 1024
POOLED_BATCH_COPY_THRESHOLD = 256
FANOUT_POOLED_BATCH_COPY_THRESHOLD = 1024
BAD = /\bwarn(?:ing)?\b|timeout|\[[WEF]\]/i
options = { implementations: IMPLEMENTATIONS, sizes: nil, readers: nil,
            output: nil, port: 45_100, pattern: 'radio-dish', recv_spin_us: nil }
OptionParser.new do |parser|
  parser.banner = 'run_shm_pubsub.rb [--pattern radio-dish|scatter-gather|client-server] [--impls LIST] [--sizes LIST]'
  parser.on('--impls LIST') { |value| options[:implementations] = value.split(',') }
  parser.on('--sizes LIST') { |value| options[:sizes] = value.split(',').map { |item| Integer(item) } }
  parser.on('--readers LIST') { |value| options[:readers] = value.split(',').map { |item| Integer(item) } }
  parser.on('--output PATH') { |value| options[:output] = File.expand_path(value) }
  parser.on('--port-base PORT', Integer) { |value| options[:port] = value }
  parser.on('--pattern PAIR', %w[radio-dish scatter-gather client-server]) { |value| options[:pattern] = value }
  parser.on('--recv-spin-us LIST') { |value| options[:recv_spin_us] = value.split(',').map { |item| Integer(item) } }
end.parse!
latency = options[:pattern] == 'client-server'
options[:readers] ||= options[:pattern] == 'radio-dish' ? [4, 32] : [1]
options[:sizes] ||= latency ? [16, 64, 256, 1024, 4096] : SIZES
options[:recv_spin_us] ||= latency ? [0, 50] : [0]
cache = { 'radio-dish' => 'pubsub', 'scatter-gather' => 'pushpull', 'client-server' => 'reqrep' }.fetch(options[:pattern])
cache_home = ENV['XDG_CACHE_HOME'].to_s
cache_home = File.expand_path('~/.cache') if cache_home.empty?
options[:output] ||= File.join(cache_home, 'omq', "shm-#{cache}.jsonl")
allowed = options[:pattern] == 'radio-dish' ? IMPLEMENTATIONS : IMPLEMENTATIONS + ['tcp']
raise 'unknown implementation' unless (options[:implementations] - allowed).empty?
raise 'invalid sizes' unless (options[:sizes] - SIZES).empty? && !options[:sizes].empty?
raise 'invalid readers' unless (options[:readers] - (options[:pattern] == 'radio-dish' ? [4, 32] : [1])).empty? && !options[:readers].empty?
raise 'invalid receive spin' unless (options[:recv_spin_us] - (options[:pattern] == 'radio-dish' ? [0] : [0, 30, 50])).empty? && !options[:recv_spin_us].empty?

def peer_command(implementation, role, size, readers, id, _port, directory, pattern, recv_spin_us)
  cpus = implementation == 'inproc' ? '1,2,3,4' : role == 'pub' ? '1,2' : '3,4'
  arguments = if pattern == 'radio-dish'
                [implementation, role, size.to_s, readers.to_s, id.to_s, directory]
              else
                [pattern, implementation, role, size.to_s, recv_spin_us.to_s, directory]
              end
  ['taskset', '-c', cpus, File.join(ENV.fetch('CARGO_TARGET_DIR'), 'release/omq_shm_bench_peer'),
   *arguments]
end

def run_case(implementation, size, readers, port, pattern, recv_spin_us)
  processes = []
  streams = {}
  rows = []
  spawn = lambda do |role, id, directory|
    input, output, error, waiter = Open3.popen3(*peer_command(implementation, role, size, readers, id, port, directory, pattern, recv_spin_us), pgroup: true)
    input.close
    processes << waiter
    streams[output] = [role, id, 'stdout', +'']
    streams[error] = [role, id, 'stderr', +'']
  end
  pump = lambda do
    next if streams.empty?
    selected = IO.select(streams.keys, nil, nil, 0.2)
    (selected&.first || []).each do |stream|
      role, id, kind, pending = streams.fetch(stream)
      chunk = stream.read_nonblock(16_384, exception: false)
      next if chunk == :wait_readable
      pending << chunk if chunk
      while (newline = pending.index("\n"))
        line = pending.slice!(0..newline)
        print "#{pattern} #{implementation} #{size}B #{readers} readers recv-spin=#{recv_spin_us} #{role}#{id}/#{kind}: #{line}"
        $stdout.flush
        raise 'peer warning or timeout' if BAD.match?(line)
        rows << JSON.parse(line) if role == 'pub' && kind == 'stdout' && line.start_with?('{')
      end
      unless chunk
        unless pending.empty?
          puts "#{role}#{id}/#{kind}: #{pending}"
          raise 'unexpected partial peer output'
        end
        streams.delete(stream)
        stream.close
      end
    end
    processes.each do |waiter|
      raise "peer failed: #{waiter.value}" if !waiter.alive? && !waiter.value.success?
    end
  end
  Dir.mktmpdir('omq-fanout-', '/mnt/bench/tmp') do |directory|
    first_role = pattern != 'radio-dish' && implementation != 'inproc' ? 'sub' : 'pub'
    spawn.call(first_role, 0, directory)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 120
    until File.exist?(File.join(directory, 'ready'))
      pump.call
      raise 'publisher setup timeout' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
    end
    spawn.call(first_role == 'pub' ? 'sub' : 'pub', 0, directory) unless implementation == 'inproc'
    until streams.empty?
      pump.call
      raise 'campaign timeout' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
    end
    processes.each { |waiter| raise 'peer failed' unless waiter.value.success? }
  end
  raise 'incomplete rounds' unless rows.map { |row| row.fetch('round') } == [1, 2, 3]
  rows.each do |row|
    raise 'wrong case' unless row['implementation'] == implementation && row['msg_size'] == size && row['readers'] == readers
    slot_bytes = [size, SLAB_BYTES].max
    pool_bytes = size >= LARGE_POOL_MIN_BODY ? LARGE_POOL_BYTES : POOL_BYTES
    slots = [pool_bytes / slot_bytes, 1].max
    slab_bytes = pattern != 'client-server' && size > 55 && size < SLAB_BYTES ? SLAB_BYTES : 0
    raise 'wrong HWM' unless row['send_hwm'] == HWM && row['recv_hwm'] == HWM
    raise 'wrong payload pool' unless row['pool_slots'] == slots && row['pool_slot_bytes'] == slot_bytes && row['payload_pool_bytes'] == slots * slot_bytes && row['slab_bytes'] == slab_bytes
    memory_cap = implementation.start_with?('shm') ? 64 * 1024 * 1024 : nil
    raise 'wrong automatic SHM memory limit' unless row['shm_max_memory'] == memory_cap
    copy_threshold = if implementation.start_with?('shm')
                       pattern == 'radio-dish' ? FANOUT_POOLED_BATCH_COPY_THRESHOLD : POOLED_BATCH_COPY_THRESHOLD
                     end
    raise 'wrong pooled batch copy policy' unless row['pooled_batch_copy_threshold'] == copy_threshold
    raise 'wrong payload fill' unless row['payload_fill'] == (pattern == 'client-server' ? 'memcpy' : 'memset')
    if pattern == 'scatter-gather'
      clock_batch = slab_bytes.positive? ? slab_bytes / size : (BATCH_BYTES / size).clamp(1, BATCH_MESSAGES)
      raise 'wrong sender clock batch' unless row['sender_clock_batch'] == clock_batch
    end
    if pattern != 'radio-dish'
      raise 'wrong spin policy' unless row['recv_spin_us'] == recv_spin_us
    end
    if pattern == 'client-server'
      raise 'wrong latency policy' unless row['socket_pair'] == pattern && row['recv_spin_us'] == recv_spin_us && row['workload_profile'] == 'latency'
      raise 'unverified latency' unless row['verified'] == true && row['samples'] == 100_000 && row['warmup_iterations'] == 20_000
      quantiles = %w[p50_us p99_us p999_us max_us].map { |field| row.fetch(field) }
      raise 'invalid latency quantiles' unless quantiles.all? { |value| value.finite? && value.positive? } && quantiles == quantiles.sort
      next
    end
    raise 'delivery mismatch' unless row['offered'].positive? && row['reader_counts'] == [row['offered']] * readers
    raise 'verification failed' unless %w[missing duplicates gaps corrupt].all? { |field| row[field] == 0 }
    raise 'bad rate' unless row['msgs_s'].finite? && row['msgs_s'].positive?
  end
  rows
ensure
  processes.each do |waiter|
    next unless waiter.alive?
    Process.kill('TERM', -waiter.pid) rescue Errno::ESRCH
  end
  sleep 0.1 if processes.any?(&:alive?)
  processes.each do |waiter|
    Process.kill('KILL', -waiter.pid) rescue Errno::ESRCH if waiter.alive?
    waiter.join
  end
  # Drain failed peers after joining them so the original diagnostic survives
  # a different peer exiting first. Normal completion already emptied streams.
  streams.each do |stream, (role, id, kind, pending)|
    next if stream.closed?
    pending << stream.read
    print "#{implementation} #{size}B #{readers} readers #{role}#{id}/#{kind}: #{pending}" unless pending.empty?
    stream.close
  end
  $stdout.flush
end

binary = File.join(ENV.fetch('CARGO_TARGET_DIR'), 'release/omq_shm_bench_peer')
revision, revision_status = Open3.capture2('git', 'rev-parse', 'HEAD')
raise 'git revision failed' unless revision_status.success?
run_id = "#{Time.now.utc.strftime('%Y%m%dT%H%M%SZ')}-shm-#{cache}"
FileUtils.mkdir_p(File.dirname(options[:output]))
options[:implementations].each_with_index do |implementation, implementation_index|
  files = [binary]
  digest = Digest::SHA256.new
  files.each { |path| File.open(path, 'rb') { |file| digest << file.read(65_536) until file.eof? } }
  options[:readers].each do |readers|
    options[:sizes].each_with_index do |size, size_index|
      options[:recv_spin_us].each do |recv_spin_us|
        port = options[:port] + implementation_index * 100 + size_index * 40
        rounds = run_case(implementation, size, readers, port, options[:pattern], recv_spin_us)
        selected = rounds.sort_by { |row| row.fetch(latency ? 'p99_us' : 'msgs_s') }[1]
        row = selected.merge('kind' => "shm_#{cache}_verified", 'impl' => implementation,
                             'transport' => implementation.split('-').first, 'peers' => readers,
                             'run_id' => run_id, 'host' => Socket.gethostname, 'revision' => revision.strip,
                             'binary_sha256' => digest.hexdigest, 'rounds' => rounds,
                             'warmup_seconds' => latency ? nil : 1, 'seconds' => latency ? nil : 3, 'drain_seconds' => 30,
                             'publisher_cpus' => implementation == 'inproc' ? '1,2,3,4' : '1,2',
                             'subscriber_cpus' => implementation == 'inproc' ? '1,2,3,4' : '3,4',
                             'subscriber_processes' => implementation == 'inproc' ? 0 : 1, 'subscriber_threads' => readers,
                             'process_layout' => implementation == 'inproc' ? 'one-process-shared-context' : 'two-process-shared-receiver',
                             'receiver_context' => 'shared',
                             'io_threads_per_context' => 1, 'inproc_data_io_threads' => 0,
                             'socket_pair' => options[:pattern],
                             'recv_mode' => options[:pattern] == 'radio-dish' ? 'yield-poll' : 'blocking',
                             'recv_spin_us' => recv_spin_us,
                             'storage' => implementation == 'shm-pool' ? 'preallocated-shm' : 'preallocated-heap',
                             'copy_in' => latency,
                             'score' => latency ? 'round-trip latency' : 'publications / max(sender elapsed, slowest reader elapsed)',
                             'selection' => latency ? 'middle p99 RTT window' : 'middle throughput window')
        File.open(options[:output], 'a') { |file| file.puts(JSON.generate(row)) }
        value = latency ? "#{row['p99_us'].round(2)} us p99 RTT" : "#{row['msgs_s'].round(1)} verified publications/s"
        puts "Saved #{options[:pattern]} #{implementation} #{size}B #{readers} readers recv-spin=#{recv_spin_us}: #{value}"
        $stdout.flush
      end
    end
  end
end
