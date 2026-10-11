# Two-process loopback rivals

Each run starts one server process and one client process. Throughput warms up
for one second (three seconds for Aeron's JVM), then records three 3-second
receiver-side timed windows. The server acknowledges each round after
consuming all sent messages. Latency warms up for 2,000 exchanges (200,000
for Aeron's JVM), then measures 10,000 client-observed round trips with one
request outstanding.
Throughput covers 16 B through 8 MiB at the 15 sizes in `moms/throughput.svg`;
latency covers the six sizes in `moms/latency_spin.svg` and `moms/latency.svg`.
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
to an 8 MiB queue budget (minimum two messages). Both peers poll; iceoryx2 latency
belongs in `moms/latency_spin.svg`. Readiness files are outside the timed path;
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

## Shared-context fan-out

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
Select cases with `--impls omq-ipc,iceoryx2-shm --sizes 16,1024 --readers 1,4`.
Each case starts two processes: one publisher and a receiver with 1/4/32
subscriber sockets/handles and one application thread per reader. OMQ sockets
share one receiver context and IO thread, matching main's PUB/SUB benchmarks.
Three 3-second windows follow a 1-second warmup. All readers must receive every message
in sequence. Payload length and sentinel are checked, not every byte. The score
includes the slowest reader's drain and is normalized per reader. Raw counts,
reader intervals, individual windows, and executable digests remain in the
append-only `mom-fanout.jsonl`. Warnings, failures, and timeouts stop the campaign.

OMQ/libzmq use no-drop fan-out, zenoh blocks on congestion, iceoryx2 disables
safe overflow. libzmq sockets share one context; zenoh subscribers share one
session and poll the native FIFO receiver. Aeron subscribers share one client and UDP image, with one SHARED
driver per process; local IPC readers share the publisher's driver. Aeron logs
use `/dev/shm`. zenoh/Aeron can coalesce network delivery before local fan-out.
Publisher CPUs 1-2 and receiver CPUs 3-4
give every implementation the same four-vCPU budget. 32 readers measures
oversubscription. Readiness, probes, and control acknowledgments use Unix sockets
outside timing; measured messages and end markers use the tested transport.
