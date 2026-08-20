# OMQ.Net

Fast, boring .NET binding for OMQ. Native transport stays in `omq-libzmq`.
Managed code owns lifetimes, copies message bytes at the boundary, and
serializes each socket handle. No Mono needed.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/dotnet/doc/charts/bindings.svg" alt="OMQ.Net performance" width="850">
</p>

## Status

Early implementation. The sync core is here: all OMQ socket types, context
sharing, bind/connect, multipart messages, send/receive, subscriptions,
RADIO/DISH groups, common options, deterministic disposal, and native errno
exceptions. Async send/receive, cancellation polling, readiness polling,
CURVE key generation, PLAIN/CURVE configuration, and socket monitor events are
included.

`Dgram` is present in the public enum but currently rejected by the shared
`omq-libzmq` ABI. Native DGRAM support must land before claiming full parity.

## Build and test

Requires .NET SDK 10 and Rust:

```sh
cargo build --release -p omq-libzmq
dotnet build bindings/dotnet/Omq.Net.csproj
LD_LIBRARY_PATH="$PWD/target/release" dotnet run --project bindings/dotnet/tests/Omq.Net.Smoke.csproj

# bounded lifecycle/race checks
LD_LIBRARY_PATH="$PWD/target/release" dotnet run --project bindings/dotnet/tests/Omq.Net.Lifecycle.csproj

# .NET <-> pyzmq protocol/security interop (NULL, CURVE, authenticated PLAIN)
LD_LIBRARY_PATH="$PWD/target/release" python3 bindings/dotnet/tests/interop.py
```

Run the real two-process TCP benchmark. It compares OMQ.Net with NetMQ, uses
the full throughput sizes, and limits latency to 4 KiB:

```sh
LD_LIBRARY_PATH="$PWD/target/release" \
  python3 bindings/dotnet/scripts/update_perf.py
```

Fast check:

```sh
LD_LIBRARY_PATH="$PWD/target/release" \
  python3 bindings/dotnet/scripts/update_perf.py --quick
```

The native library can also be supplied through the normal loader paths. Keep
`libomq_zmq.so` beside the test process or set `LD_LIBRARY_PATH`.

## API

```csharp
using Omq;

using var context = new Context();
using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0 });
using var push = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0 });

string endpoint = pull.Bind("tcp://127.0.0.1:5555");
push.Connect(endpoint);
push.Send(Message.Text("hello"));
Console.WriteLine(pull.Receive().ToString());
```

Socket methods are safe across managed threads. A call holds the socket gate
until its native operation completes. `Dispose` closes the native handle once;
message frames are always closed in `finally` blocks. This is the baseline for
race and leak tests before adding zero-copy or async APIs.

## Performance contract

Benchmark charts compare sync and async OMQ.Net against sync and async NetMQ
over two-process loopback TCP. PUSH/PULL throughput spans 16 B through 32 KiB;
REQ/REP p50 latency spans 16 B through 4 KiB. Results append to:
`~/.cache/omq.net/bindings.jsonl`. The cache is never rewritten; chart-only
regeneration selects the newest row for each implementation, pattern, and size.

```sh
python3 bindings/dotnet/scripts/update_perf.py --chart-only
```

Current quick-run result on this VM: OMQ.Net beats NetMQ at every sampled size.
Treat that as a measurement, not a promise. Full runs need 3 rounds on a quiet
machine before publishing a performance claim.
