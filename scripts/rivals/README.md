# Two-process loopback rivals

Each run starts one server process and one client process. Throughput warms up
for one second (three seconds for Aeron's JVM), then records three 3-second
receiver-side timed windows. The server acknowledges each round after
consuming all sent messages. Latency warms up for 2,000 exchanges (200,000
for Aeron's JVM), then measures 10,000 client-observed round trips with one
request outstanding.
Network throughput covers 16 B through 8 MiB in `moms/throughput_network.svg`;
RTT is in `moms/latency_network.svg`. Local iceoryx2 results are in the
corresponding `_local.svg` charts.
The wrapper appends one row per requested size only after every case in that
invocation passes.

| implementation key | link | workload |
|---|---|---|
| `aeron-udp-2proc` | loopback UDP | Aeron message publication, embedded shared-thread Media Driver per process |
| `zenoh-tcp-2proc` | direct loopback TCP peer link | zenoh pub/sub with blocking congestion control |
| `iroh-quic-2proc` | direct loopback QUIC | one uni stream per throughput round, one bi stream for latency |
| `iceoryx2-shm-2proc` | same-host shared memory | iceoryx2 0.10.0 pub/sub byte slices, copy-in and borrowed receive, continuous polling |

The client sends continuously during each timed window. The server counts only
complete messages received during its window, then drains the remainder and
verifies the total received count. Aeron uses 64 MiB terms, allowing 8 MiB
messages; its UDP fragments are reassembled before counting or echoing.

Iroh streams bytes rather than delivering discrete messages. Aeron and zenoh
deliver messages. No result here implies equal delivery or persistence
semantics.

iceoryx2 uses two one-way pub/sub services, with one publisher and subscriber
each. Every send copies the full byte buffer into a newly loaned SHM sample.
Receive borrows the sample until its handle is dropped; throughput checks size,
sequence, and the last byte, while latency validates the full echo. This measures
copy-in IPC, not a metadata-only or application loan benchmark. Safe overflow and
history are disabled. Queue capacity is 256 messages, reduced for large payloads
to an 8 MiB queue budget (minimum two messages). Both peers poll; iceoryx2 RTT
belongs in `moms/latency_local.svg`. Readiness files are outside the timed path;
data, round markers, acknowledgments, and echoes use SHM.
Its rates use the actual receiver interval ending at the last included batch;
boundary-crossing batches are excluded. JSONL retains its individual rounds.

Build on the mounted benchmark disk. Do not run rival processes concurrently
with other benchmarks. Stop on any warning or timeout.

```sh
export CARGO_TARGET_DIR=/mnt/bench/tmp/cargo-target
export TMPDIR=/mnt/bench/tmp
cargo fmt --manifest-path scripts/rivals/Cargo.toml
cargo clippy --manifest-path scripts/rivals/Cargo.toml --all-targets -- -D warnings
cargo build --manifest-path scripts/rivals/Cargo.toml --release
cargo fmt --manifest-path scripts/iceoryx2_bench_peer/Cargo.toml
cargo clippy --manifest-path scripts/iceoryx2_bench_peer/Cargo.toml --all-targets -- -D warnings
cargo build --manifest-path scripts/iceoryx2_bench_peer/Cargo.toml --release

export AERON_JAR=/mnt/bench/tmp/aeron-all-1.51.0.jar
export AERON_CLASSES=/mnt/bench/tmp/aeron-classes
curl -fsSL https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.51.0/aeron-all-1.51.0.jar -o "$AERON_JAR"
mkdir -p "$AERON_CLASSES"
ln -sf "$(command -v java)" "$CARGO_TARGET_DIR/release/omq_aeron_peer"
javac -cp "$AERON_JAR" -d "$AERON_CLASSES" scripts/rivals/AeronUdpPeer.java

python3 scripts/rivals/run_two_process.py aeron --pin
python3 scripts/rivals/run_two_process.py zenoh --pin
python3 scripts/rivals/run_two_process.py iroh --pin
python3 scripts/rivals/run_two_process.py iceoryx2 --pin
cargo run -p omq-bench -- chart mom
```

The wrapper uses base port 43100 by default, plus two per case. Override with
`--port-base` if another process occupies that range. Set `--output` to write
to a separate JSONL file for a trial run. Use unique ports for concurrent
development runs, but keep timed benchmark runs sequential.
`--pin` places the client on vCPUs 1-2 and server on vCPUs 3-4.
`--mode latency --sizes 16,32` selects a subset for repeated passes.

## Verified fan-out

```sh
cargo fmt --manifest-path scripts/fanout_bench_peer/Cargo.toml
cargo clippy --manifest-path scripts/fanout_bench_peer/Cargo.toml --all-targets -- -D warnings
cargo build --release --manifest-path scripts/fanout_bench_peer/Cargo.toml
javac -Xlint:all -Werror -cp "$AERON_JAR" -d "$AERON_CLASSES" scripts/rivals/AeronFanoutPeer.java
ruby scripts/rivals/run_fanout.rb
omq_bench chart mom
```

Use the same exported target/JAR/classes paths above. QUIC uses the credentials
created by an earlier `omq_bench run dart --transport quic` run.
Select cases with `--impls omq-ipc,omq-shm,omq-shm-pool,iceoryx2-shm --sizes 16,1024 --readers 1,4`.
OMQ runs blocking bulk receive with 0/50 us receive spin, interleaved per case;
`--recv-spin-us 0` or `--recv-spin-us 50` selects one budget. IO spin stays zero.
Use `omq_bench chart mom-fanout` to refresh only the two fan-out charts.
Most cases start one publisher and one receiver process with one application
thread per reader. Aeron 32-reader cases start 32 separate receiver processes.
OMQ sockets share one receiver context and IO thread.
Three 3-second windows follow a 1-second warmup. All readers must receive every message
in sequence. Payload length and sentinel are checked, not every byte. The score
includes the slowest reader's drain and is normalized per reader. Raw counts,
reader intervals, individual windows, and executable digests remain in the
append-only `mom-fanout.jsonl`. Warnings, failures, and timeouts stop the campaign.

OMQ SHM uses RADIO/DISH. Ordinary messages include automatic SHM preparation;
`omq-shm-pool` fills explicit SHM slots, retaining them until all readers
release their views. Small bodies remain inline. Receivers borrow frame slices
for verification. The automatic outbound region limit is 64 MiB per socket.
Large-size comparisons accept `--sizes 131072,1048576,8388608`.
OMQ uses HWM 1000 at every size. Rival queue capacity is
`clamp(32 MiB / size, 4, 256)`; producer pool slots independently use
`clamp(64 MiB / size, 4, 1024)`.

OMQ/libzmq use no-drop fan-out, zenoh blocks on congestion, iceoryx2 disables
safe overflow. Aeron 32-reader UDP uses dynamic multi-destination cast and a
Media Driver per receiver; IPC readers share the publisher's mapped driver.
Publisher CPUs 1-2 and receiver CPUs 3-4
give every implementation the same four-vCPU budget. 32 readers measures
oversubscription. Readiness, probes, and control acknowledgments use Unix sockets
outside timing; measured messages and end markers use the tested transport.
