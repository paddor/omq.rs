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

SIZES = [16, 64, 256, 1024, 4096, 16_384].freeze
IMPLEMENTATIONS = %w[omq-tcp omq-quic omq-dart libzmq-tcp zenoh-tcp aeron-udp omq-ipc libzmq-ipc iceoryx2-shm aeron-ipc].freeze
BAD = /\bwarn(?:ing)?\b|timeout|\[[WEF]\]/i
options = { implementations: IMPLEMENTATIONS, sizes: SIZES, readers: [1, 4, 32],
            output: File.expand_path('~/.cache/omq/mom-fanout.jsonl'), port: 45_100 }
OptionParser.new do |parser|
  parser.banner = 'run_fanout.rb [--impls omq-tcp,...] [--sizes 16,...] [--readers 1,4,32]'
  parser.on('--impls LIST') { |value| options[:implementations] = value.split(',') }
  parser.on('--sizes LIST') { |value| options[:sizes] = value.split(',').map { |item| Integer(item) } }
  parser.on('--readers LIST') { |value| options[:readers] = value.split(',').map { |item| Integer(item) } }
  parser.on('--output PATH') { |value| options[:output] = File.expand_path(value) }
  parser.on('--port-base PORT', Integer) { |value| options[:port] = value }
end.parse!
raise 'unknown implementation' unless (options[:implementations] - IMPLEMENTATIONS).empty?
raise 'invalid sizes' unless (options[:sizes] - SIZES).empty? && !options[:sizes].empty?
raise 'invalid readers' unless (options[:readers] - [1, 4, 32]).empty? && !options[:readers].empty?

def peer_command(implementation, role, size, readers, id, port, directory)
  arguments = [implementation, role, size.to_s, readers.to_s, id.to_s, port.to_s, directory]
  command = if implementation.start_with?('aeron-')
              [File.join(ENV.fetch('CARGO_TARGET_DIR'), 'release/omq_aeron_peer'),
               '--add-opens', 'java.base/jdk.internal.misc=ALL-UNNAMED',
               '-XX:ActiveProcessorCount=1', '-Xms32m', '-Xmx128m', '-cp',
               "#{ENV.fetch('AERON_CLASSES')}:#{ENV.fetch('AERON_JAR')}", 'AeronFanoutPeer']
            else
              [File.join(ENV.fetch('CARGO_TARGET_DIR'), 'release/omq_fanout_peer')]
            end
  ['taskset', '-c', role == 'pub' ? '1,2' : '3,4', *command, *arguments]
end

def run_case(implementation, size, readers, port)
  processes = []
  streams = {}
  rows = []
  spawn = lambda do |role, id, directory|
    input, output, error, waiter = Open3.popen3(*peer_command(implementation, role, size, readers, id, port, directory), pgroup: true)
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
        print "#{implementation} #{size}B #{readers} readers #{role}#{id}/#{kind}: #{line}"
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
    if implementation == 'omq-quic'
      credentials = File.expand_path('~/.cache/omq/bench-tls')
      FileUtils.cp(File.join(credentials, 'cert.pem'), directory)
      FileUtils.cp(File.join(credentials, 'key.pem'), directory)
    end
    spawn.call('pub', 0, directory)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 120
    until File.exist?(File.join(directory, 'ready'))
      pump.call
      raise 'publisher setup timeout' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
    end
    spawn.call('sub', 0, directory)
    until streams.empty?
      pump.call
      raise 'campaign timeout' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
    end
    processes.each { |waiter| raise 'peer failed' unless waiter.value.success? }
  end
  raise 'incomplete rounds' unless rows.map { |row| row.fetch('round') } == [1, 2, 3]
  rows.each do |row|
    raise 'wrong case' unless row['implementation'] == implementation && row['msg_size'] == size && row['readers'] == readers
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
  streams.each_key { |stream| stream.close unless stream.closed? }
end

binary = File.join(ENV.fetch('CARGO_TARGET_DIR'), 'release/omq_fanout_peer')
revision, revision_status = Open3.capture2('git', 'rev-parse', 'HEAD')
raise 'git revision failed' unless revision_status.success?
run_id = "#{Time.now.utc.strftime('%Y%m%dT%H%M%SZ')}-fanout"
FileUtils.mkdir_p(File.dirname(options[:output]))
options[:implementations].each_with_index do |implementation, implementation_index|
  files = if implementation.start_with?('aeron-')
            [ENV.fetch('AERON_JAR'), File.join(ENV.fetch('AERON_CLASSES'), 'AeronFanoutPeer.class')]
          else
            [binary]
          end
  digest = Digest::SHA256.new
  files.each { |path| File.open(path, 'rb') { |file| digest << file.read(65_536) until file.eof? } }
  options[:readers].each do |readers|
    options[:sizes].each_with_index do |size, size_index|
      port = options[:port] + implementation_index * 100 + size_index * 40
      rounds = run_case(implementation, size, readers, port)
      selected = rounds.sort_by { |row| row.fetch('msgs_s') }[1]
      row = selected.merge('kind' => 'pub_sub_verified', 'impl' => implementation,
                           'transport' => implementation.split('-').last, 'peers' => readers,
                           'run_id' => run_id, 'host' => Socket.gethostname, 'revision' => revision.strip,
                           'binary_sha256' => digest.hexdigest, 'rounds' => rounds,
                           'warmup_seconds' => 1, 'seconds' => 3, 'drain_seconds' => 30,
                           'publisher_cpus' => '1,2', 'subscriber_cpus' => '3,4',
                           'subscriber_processes' => 1, 'subscriber_threads' => readers,
                           'process_layout' => 'two-process-shared-receiver',
                           'receiver_context' => implementation.start_with?('iceoryx2-') ? 'per-thread-node' : 'shared',
                           'io_threads_per_context' => implementation.start_with?('iceoryx2-') ? 0 : implementation.start_with?('zenoh-') ? nil : 1,
                           'copy_in' => true,
                           'score' => 'publications / max(sender elapsed, slowest reader elapsed)',
                           'selection' => 'middle throughput window')
      File.open(options[:output], 'a') { |file| file.puts(JSON.generate(row)) }
      puts "Saved #{implementation} #{size}B #{readers} readers: #{row['msgs_s'].round(1)} verified publications/s"
      $stdout.flush
    end
  end
end
