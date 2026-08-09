# OMQ.java

Modern Java bindings for OMQ backed by `omq-tokio`.

This is not a JeroMQ compatibility layer. The Java API owns a native OMQ
context, and that context owns the background IO thread(s), matching the normal
libzmq architecture.

Architecture detail: [`doc/architecture.md`](doc/architecture.md).

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

## Shape

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
