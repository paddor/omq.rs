# Standalone Quinn DATAGRAM benchmark

Measured 2026-10-07 on the Linux VM described in `.chart_hw`: i7-8700B,
6 cores, performance governor, turbo disabled. Two independent IPv4 loopback
processes, pinned to CPUs 1 and 3, each running one Tokio current-thread
runtime. The application runs as a task alongside Quinn's drivers.

This uses Quinn 0.11.12 / quinn-proto 0.11.19 directly, without OMQ queues,
framing, routing, or a stream wrapper. TLS 1.3 uses verified server certificates
and AES-128-GCM through rustls/ring. The default Cubic controller, packet ACKs,
pacing, ECN handling, and GSO/GRO remain enabled. Loopback does not establish
ECN behavior under actual network congestion.

## Results

Medians of three serial runs for each configuration and body size. Throughput
is received application messages during a 3-second window; 200 ms warmup and
300 ms receiver tail. Latency is echo RTT, with 20,000 warmup exchanges followed
by 100,000 measured exchanges. Every latency run had zero timeouts.
GB/s is decimal payload bandwidth.

| UDP adapter | Body | Received M/s | Payload GB/s | RTT p50 us | RTT p99 us | RTT p99 range us |
|---|---:|---:|---:|---:|---:|---:|
| Stock Tokio | 16 B | 3.416 | 0.055 | 53.388 | 64.531 | 64.187-65.737 |
| Stock Tokio | 1 KiB | 0.582 | 0.596 | 56.088 | 65.794 | 65.188-68.752 |
| UDP probing, 50 us idle budget | 16 B | 3.311 | 0.053 | 28.075 | 63.640 | 39.580-64.520 |
| UDP probing, 50 us idle budget | 1 KiB | 0.417 | 0.427 | 30.985 | 51.012 | 43.125-60.920 |

Increasing the stock application's drain budget from 256 to 1,024 messages
(still capped at 64 KiB) raised 16 B received throughput to a median 3.651 M/s,
with runs at 3.602, 3.651, and 3.703 M/s. Offered throughput was 8.69-8.75 M/s;
57-58% of measured messages were missing at the application. Thus the offered
rate is not the delivery ceiling. The evaluated 5 M/s at 16 B, 1 GB/s at 1 KiB,
and RTT p99 <=25 us were not reached, including the best individual runs.
These are results on this machine, not a universal Quinn limit.

The default 16 B runs lost 48-51% of offered messages with the stock adapter,
and 20-26% with UDP probing. Both peers' QUIC DATAGRAM frame totals matched,
and the sender reported zero lost packets: these drops were in the bounded
application receive queue. `send_datagram_wait()` only bounds the local send
queue; DATAGRAM has no receiver flow control. At 1 KiB the probing runs had
zero missing messages; two stock runs had zero, and one lost 3,096 (0.177%).
No retransmission was added for DATAGRAM bodies.

Both adapters advertised GSO/GRO capacity 64. Requested socket buffers were
8 MiB; Linux reported 425,984 B after applying its kernel caps. The current
path MTU reached 1,452 B. Throughput bodies are immutable reusable `Bytes`
with distinct warmup/measurement tags, excluding sender payload allocation.
The receive count does not claim application-level unique sequence delivery.
Echo requests include a unique u64 sequence and validate the full reply.

Two-worker Tokio and separate application/IO layouts were also explored with
shorter runs. Neither improved the measured ceiling. Spinning the application
on a separate core repeatedly acquired Quinn's connection mutex and made
latency worse. Polling the reactor every scheduler turn also made the direct
UDP adapter slower than the default interval of 61.

## What the profiles show

Profiles were collected separately from the reported runs, at 997 Hz with
DWARF call stacks and zero lost samples. In the 1 KiB stock sender profile,
AES/GCM symbols accounted for about 20% of sampled CPU cycles; packet
construction, sent-packet bookkeeping, copying, and kernel work also cost CPU.
In the 16 B polling latency profile, syscall entry/exit and receive polling
dominated, with individual crypto symbols below 1%.

This does not isolate the exact encryption cost on the RTT critical path.
Removing encryption alone is not supported as an explanation or solution for
the entire measured gap. Authentication tags would still require computation.

## Implications for Dart

Quinn DATAGRAM supplies packet protection and QUIC congestion/ECN machinery,
but messages remain unreliable and have no receiver flow control. This run
does not support replacing Dart with it for the evaluated performance.
An Aeron-like reliable transport still needs receiver credits and bounded
loss recovery, including a timer for the final lost message.

A ring can serve as the retransmission log if it has independent send and
acknowledgment/reclamation cursors. Current yring 0.3.20 `pop()` moves the
message out; delayed `release()` reserves capacity, but does not keep the
popped value available for retransmission. Its `release()` also publishes all
popped slots rather than an acknowledged prefix. Keeping owned messages in
a bounded retained queue would work today; borrowing retained slots and
releasing through a cumulative ACK would require a queue API extension in
the external fanring.rs workspace.

For an unencrypted authenticated transport, authentication must cover the
transport headers and ACK/credit control traffic as well as the payload.
An opaque application payload tag alone does not authenticate that control
plane.

## Reproduction and artifacts

Commands and runner options are in
[RUNNING_BENCHMARKS.md](../RUNNING_BENCHMARKS.md#standalone-quinn-datagram).
Full rows append to `~/.cache/omq/quinn-datagram.jsonl`.

The final benchmark binary is preserved at
`/mnt/bench/tmp/quinn-datagram-final-20261007`, SHA256
`d60e6bc522e5420f578e76ba93be9faae1855bf0c38974c5c4635bfccb4e2d23`.
Stock default rows predate adding runtime comparison options and have digest
`c80511ce617c4c753537c55eef85d9428df0fb784034ade51356ec97ac61a7a2`;
the measured message loops are the same. Every row records its binary digest.
The expanded-batch rows use the final binary.

Profiles and exploratory data are under `/mnt/bench/tmp/`:

- `quinn-datagram-profile-1k/send-1024.data`
- `quinn-datagram-profile-latency/client-16.data`
- `quinn-datagram-profile-stock/receive-16.data`
- `quinn-datagram-profile-20261007.jsonl`
- `quinn-datagram-setup-20261007.jsonl`

The first receiver profile exposed per-message deadline timer overhead in
the initial harness; the measured harness uses one throughput deadline timer
for the entire receive loop. Earlier exploratory rows also include a reactor
keep-awake experiment and a root-future application layout. They are excluded
from the reported three-run results.
