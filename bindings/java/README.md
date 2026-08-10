# OMQ.java

OMQ.java brings OMQ sockets to Java with a native Rust backend.

It wraps `omq-tokio`, so routing, reconnect, fair-queueing, auth, compression,
and transport I/O run in OMQ-owned background threads while Java code gets a
small API built around `AutoCloseable`, `Duration`, `ByteBuffer`, and
`CompletableFuture`.

## Highlights

- Native OMQ engine shared with OMQ.rs.
- High-throughput `tcp://`, native `ipc://`, and `inproc://` messaging.
- Compression transports: `lz4+tcp://` and `zstd+tcp://`.
- Static compression dictionaries and auto-trained dictionaries.
- PLAIN and CURVE security with Java auth callbacks and peer metadata.
- Java 25 FFM off-heap rings hide batching behind normal scalar send/receive
  calls.
- Explicit native context sharing for `inproc://` across Java handles.

## Performance

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/java/doc/charts/pushpull_tcp.svg" alt="OMQ.java vs JeroMQ TCP throughput and latency" width="850">
</p>

2-process loopback PUSH/PULL throughput and REQ/REP p50 latency vs JeroMQ
over TCP. Both panels warm the JVM before timed samples. Results are median
local runs from `scripts/bench_pushpull_tcp.py`.

## Build, install, test

Requires Java 25 or newer. JPMS module name: `io.omq`.

Maven Central coordinates. Use the latest version listed on Maven Central:

```xml
<dependency>
  <groupId>io.github.paddor</groupId>
  <artifactId>omq-java</artifactId>
  <version>VERSION</version>
</dependency>
```

Synchronous receives use Java 25 FFM and require native access:

```sh
--enable-native-access=ALL-UNNAMED
```

If the jar is used on the module path, enable only the automatic module:

```sh
--enable-native-access=io.omq
```

```sh
mvn package
mvn install
mvn test
```

Maven builds the Rust native library in `native/target/debug` and embeds the
current-platform native library in the jar under `io/omq/native/...`.

## API Shape

- `Context` owns native IO threads and creates sockets.
- `Context.shareKey()` / `Context.fromShareKey(...)` explicitly share one
  native context core and `inproc://` namespace across Java handles.
- `SocketOptions` builds reusable pre-I/O option sets for socket creation.
- `Socket` is `AutoCloseable`; use try-with-resources.
- `Message` is immutable and supports single-part and multipart payloads.
- `receiveBytes` is the direct single-part hot path; use `receive` when
  multipart metadata matters.
- Sync receive methods transparently drain a Java 25 FFM off-heap ring filled
  from native `recv_many_into()`, so scalar `receive*` calls amortize native
  transition cost without exposing batch APIs.
- Blocking receives on virtual threads drain cached ring data first and then
  park through a native async receive when empty.
- `sendAsync` and `receiveAsync` return `CompletableFuture` values backed by
  native OMQ runtime tasks, not Java worker threads. Cancel the returned
  future to abort the native task.
- Sockets are synchronized on the Java side. Treat a socket as a single-thread
  object; create more sockets for more concurrent flows.

OMQ.java is not a JeroMQ compatibility layer. It follows ZMQ socket semantics,
but exposes a modern Java API instead of mirroring JeroMQ classes.

Architecture detail: [`doc/architecture.md`](doc/architecture.md).

Example:

```java
try (Context ctx = OMQ.context();
     Socket pull = ctx.socket(SocketType.PULL);
     Socket push = ctx.socket(SocketType.PUSH)) {
    String endpoint = pull.bind("tcp://127.0.0.1:0");
    push.connect(endpoint);
    push.send("hello");
    String body = pull.receive(Duration.ofSeconds(5)).orElseThrow().text();
}
```

Reusable socket options:

```java
SocketOptions options = SocketOptions.builder()
        .sendHighWaterMark(10_000)
        .heartbeatInterval(Duration.ofSeconds(5))
        .build();

try (Context ctx = OMQ.context();
     Socket push = ctx.socket(SocketType.PUSH, options)) {
    push.connect("tcp://127.0.0.1:5555");
}
```

Async receive from multiple distinct sockets uses a typed helper:

```java
ReceiveEvent event = OMQ.receiveAny(socketA, socketB).get();
Socket ready = event.socket();
Message first = event.message();
```
