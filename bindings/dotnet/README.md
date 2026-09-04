# OMQ.Net

Fast .NET binding for OMQ, backed by the `omq-libzmq` C ABI. Managed handles
own native lifetimes; socket calls are serialized and message bytes are copied
at the boundary.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/dotnet/doc/charts/bindings.svg" alt="OMQ.Net performance" width="850">
</p>

2-process loopback TCP comparison against NetMQ. Throughput covers 16 B–32 KiB;
REQ/REP p50 latency covers 16 B–4 KiB. See [`DEVELOPMENT.md`](DEVELOPMENT.md)
for benchmark and regeneration commands.

## Install

```sh
dotnet add package Omq.Net
```

The package targets `net8.0` and `net10.0`, and includes RID-specific native
assets for supported platforms.

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

The public API includes all OMQ socket types, multipart messages, options,
polling, monitors, proxies, CURVE key helpers, synchronous operations, and
cancellation-aware async operations. Generated XML documentation provides
IntelliSense summaries for the public API.

OMQ.Net follows OMQ/libzmq socket semantics; it is not a NetMQ compatibility
layer.

PLAIN servers require fixed credentials through
`socket.ConfigurePlainServer("alice", "secret")`. Clients use
`ConfigurePlainClient`. PLAIN authenticates but does not encrypt traffic.

## Development

Architecture: [`doc/architecture.md`](doc/architecture.md).

Build, test, benchmark, and packaging instructions: [`DEVELOPMENT.md`](DEVELOPMENT.md).
