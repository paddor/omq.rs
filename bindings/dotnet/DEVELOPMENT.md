# OMQ.Net development

Requires the .NET SDK 10 and a Rust toolchain. Mono is not required.

## Build and test

From the repository root:

```sh
cargo build --release -p omq-libzmq
dotnet build bindings/dotnet/Omq.Net.csproj

LD_LIBRARY_PATH="$PWD/target/release" \
  dotnet run --project bindings/dotnet/tests/Omq.Net.Smoke.csproj

LD_LIBRARY_PATH="$PWD/target/release" \
  dotnet run --project bindings/dotnet/tests/Omq.Net.Lifecycle.csproj

LD_LIBRARY_PATH="$PWD/target/release" \
  python3 bindings/dotnet/tests/interop.py
```

The interop suite covers .NET ↔ pyzmq NULL, CURVE, and authenticated PLAIN
TCP peers. Lifecycle tests cover cancellation, bounded HWM waits, monitor
shutdown, connect-before-bind, and repeated disposal.

CI runs the managed binding checks on Linux. The release workflow builds
native assets for Linux x64, macOS x64, macOS arm64, and Windows x64.

## Benchmarks

Run the full two-process TCP comparison against NetMQ:

```sh
LD_LIBRARY_PATH="$PWD/target/release" \
  python3 bindings/dotnet/scripts/update_perf.py
```

Use `--quick` for a short check. Throughput measures 16 B–32 KiB; latency
measures 16 B–4 KiB. Results append to `~/.cache/omq.net/bindings.jsonl`.
Regenerate only the chart with:

```sh
python3 bindings/dotnet/scripts/update_perf.py --chart-only
```

## NuGet packaging

Build native libraries for each RID, then pass `RID=PATH` arguments:

```sh
bash bindings/dotnet/scripts/package.sh /tmp/omq-nuget \
  linux-x64=target/release/libomq_zmq.so
```

The package contains managed assemblies, generated XML documentation, the
README, and native assets under NuGet RID folders. Release publication uses
`.github/workflows/release-dotnet.yml` and NuGet trusted publishing.
