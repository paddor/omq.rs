# Two-process loopback rivals

Each run starts one server process and one client process. The client records
three measured rounds after warmup. Throughput includes receiver completion:
the server sends an acknowledgment after consuming each round. Latency is the
client-observed round trip with one request outstanding. Throughput covers
16 B through 8 MiB at the 15 sizes in `main_mom_tcp.svg`; latency covers the
six sizes in `main_mom_latency_tcp.svg`. The wrapper appends one median row
per size to `~/.cache/omq/comparisons.jsonl` only after all 21 cases pass.

| implementation key | link | workload |
|---|---|---|
| `aeron-udp-2proc` | loopback UDP | Aeron message publication, embedded shared-thread Media Driver per process |
| `zenoh-tcp-2proc` | direct loopback TCP peer link | zenoh pub/sub with blocking congestion control |
| `iroh-quic-2proc` | direct loopback QUIC | one uni stream per throughput round, one bi stream for latency |

Measured throughput rounds send up to 256 MiB, with a minimum of 32 messages
and a cap of 4 million Aeron, 2 million iroh, or 1 million zenoh messages.
Warmup transfers at most 8 MiB. Aeron uses 64 MiB terms, allowing 8 MiB
messages; its UDP fragments are reassembled before counting or echoing.

Iroh streams bytes rather than delivering discrete messages. Aeron and zenoh
deliver messages. No result here implies equal delivery or persistence
semantics.

Build on the mounted benchmark disk. Do not run rival processes concurrently
with other benchmarks. Stop on any warning or timeout.

```sh
export CARGO_TARGET_DIR=/mnt/bench/tmp/cargo-target
export TMPDIR=/mnt/bench/tmp
cargo fmt --manifest-path scripts/rivals/Cargo.toml
cargo clippy --manifest-path scripts/rivals/Cargo.toml --all-targets -- -D warnings
cargo build --manifest-path scripts/rivals/Cargo.toml --release

export AERON_JAR=/mnt/bench/tmp/aeron-all-1.51.0.jar
export AERON_CLASSES=/mnt/bench/tmp/aeron-classes
curl -fsSL https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.51.0/aeron-all-1.51.0.jar -o "$AERON_JAR"
mkdir -p "$AERON_CLASSES"
javac -cp "$AERON_JAR" -d "$AERON_CLASSES" scripts/rivals/AeronUdpPeer.java

python3 scripts/rivals/run_two_process.py aeron
python3 scripts/rivals/run_two_process.py zenoh
python3 scripts/rivals/run_two_process.py iroh
cargo run -p omq-bench -- chart main
```

The wrapper uses base port 43100 by default, plus two per case. Override with
`--port-base` if another process occupies that range. Set `--output` to write
to a separate JSONL file for a trial run. Use unique ports for concurrent
development runs, but keep timed benchmark runs sequential.
