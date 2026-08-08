# OMQ.java

Modern Java bindings for OMQ backed by `omq-tokio`.

This is not a JeroMQ compatibility layer. The Java API owns a native OMQ
context, and that context owns the background IO thread(s), matching the normal
libzmq architecture.

Architecture detail: [`doc/architecture.md`](doc/architecture.md).

## Build, install, test

```sh
mvn package
mvn install
mvn test
```

Maven builds the Rust JNI library in `native/target/debug` and embeds the
current-platform native library in the jar under `io/omq/native/...`.

## Shape

- `Context` owns native IO threads and creates sockets.
- `Socket` is `AutoCloseable`; use try-with-resources.
- `Message` is immutable and supports single-part and multipart payloads.
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
