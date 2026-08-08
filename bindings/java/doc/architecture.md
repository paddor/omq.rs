# OMQ.java Architecture

Maven/JNI binding for `omq-tokio`. The public API is synchronous Java 17.
Rust owns the OMQ context, sockets, protocol state, queues, reconnect logic,
auth, compression, and background I/O.

## Source layout

```
src/main/java/io/omq/
  OMQ.java             static entry points and CURVE helpers
  Context.java         native context handle, socket ownership, cleanup
  Socket.java          synchronized socket facade and option setters
  Message.java         immutable single-part and multipart payloads
  Native.java          JNI declarations and packaged native library loader
  *Exception.java      unchecked exception hierarchy

native/src/
  lib.rs              JNI implementation over omq_tokio::blocking
  bin/peer.rs         OMQ.rs peer used by Java interop tests
```

## Threading model

```
Java caller thread -> synchronized Socket -> JNI -> omq_tokio::blocking::Socket
                                                     |
                                                     v
                                  OMQ Context background I/O thread(s)
                                  connection tasks, queues, ZMTP, auth,
                                  compression, reconnect, transport I/O
```

`Context(int ioThreads)` creates a native `omq_tokio::Context` with that
I/O thread count. Java holds an opaque native handle. Closing the Java
context terminates the native context and closes all Java sockets created
from it.

`Socket` holds an opaque native socket handle. The Rust side stores
`Options` and lazily creates the actual `omq_tokio::blocking::Socket` on
first `bind`, `connect`, `send`, `receive`, or wait operation. Option
methods only edit the Rust `Options` value and must run before
materialization.

Java socket methods are `synchronized`, so one Java wrapper cannot race
itself. Native state uses an atomic handle for idempotent close and Rust
mutexes for materialization and option mutation. `Cleaner` is a leak
fallback, not the normal lifecycle path. Use try-with-resources.

## Virtual threads

OMQ.java does not use Java virtual threads internally. If application code
calls this API from a virtual thread, the Java call enters blocking JNI.
The JVM cannot unmount that virtual thread while native Rust is blocked,
so the carrier thread may be pinned until the OMQ operation returns.

Virtual threads are therefore a caller convenience here, not the binding's
scaling mechanism. The scaling mechanism is still OMQ-owned background I/O
threads plus native socket queues.

## Data path

Messages cross JNI as copied `byte[]` arrays. `ByteBuffer` inputs read
the remaining bytes into a byte array without changing the caller's buffer
position. Java does not encode ZMTP, manage connections, or run transport
threads. Native OMQ receives complete `Message` values and performs all
socket semantics in Rust.

Synchronous single-part receives cross JNI as a raw `byte[]`, so the common
path avoids an extra `byte[][]` allocation. `receiveBytes` returns that fresh
native-owned array directly. Multipart receives still cross as `byte[][]`.

`inproc://` is not special-cased in Java. The Rust context owns the process
inproc registry and decides whether an endpoint is local, what socket types
are compatible, and which native queue path is legal. A Java-private inproc
registry would split address ownership from Rust and would break mixed
Java/Rust endpoints inside the same process.

`trySend` calls native `try_send` and returns `false` when native routing
or high-water-mark state cannot accept the message immediately. Other
errors still raise typed exceptions. `tryReceive` and `tryRecv` return an
empty `Optional` when no complete message is available.

## Async API

`sendAsync` and `receiveAsync` return Java `CompletableFuture` values but
do not use Java worker threads. The Java method creates a future, JNI takes
a global reference to it, clones the native async socket, and spawns a Rust
future on the OMQ context runtime. JNI returns a native abort token stored
inside the future; `cancel`, user `complete`, and user `completeExceptionally`
drop that token and abort the native task. When the Rust future completes,
the OMQ runtime thread attaches to the JVM as a daemon and calls `complete`
or `completeExceptionally`.

`receiveAsync(Duration)` wraps native `recv()` in `tokio::time::timeout`
and completes exceptionally with `TimeoutException` on deadline. `sendAsync`
completes after OMQ accepts the message into outbound routing buffers, same
semantic point as synchronous `send`.

`OMQ.receiveAny(Socket...)` requires distinct sockets, races one native
`recv()` future per socket, and returns a `ReceiveEvent` with the winning
socket and message. Loser receives are aborted inside Rust before they can
consume later messages. Canceling the Java future aborts all native receives.

## Compression

Compression is enabled by endpoint scheme and native Cargo features.
The Maven build enables `lz4` and `zstd`, so Java can use endpoints such
as `lz4+tcp://127.0.0.1:5555` and `zstd+tcp://127.0.0.1:5555`.
`compressionAutoTrain`, `compressionThreshold`, and `compressionLevel`
map to OMQ options before socket materialization.

## Security

PLAIN and CURVE are enabled by the Maven native build. `OMQ.curveKeypair()`
calls `omq_proto::CurveKeypair::generate()` in Rust and returns Z85 public
and secret keys. `OMQ.curvePublic(secretKey)` parses a Z85 secret key in
Rust and derives the matching Z85 public key.

## Exceptions

All OMQ Java exceptions are unchecked.

- `TimeoutException`: timeout or would-block.
- `ClosedException`: closed context or socket.
- `InvalidEndpointException`: invalid endpoint or unsupported scheme.
- `ProtocolException`: ZMTP protocol violation, bad handshake, or
  unsupported ZMTP version.
- `TransportException`: base for endpoint transport I/O failures. Carries
  `operation()`, `endpoint()`, and native `detail()`.
- `BindException`: native bind I/O failure, for example address in use.
- `ConnectException`: native connect preflight I/O failure.
- `NameResolutionException`: host lookup failure during bind, connect,
  unbind, or disconnect.
- `OMQException`: invalid configuration, message-too-large, unroutable,
  JNI/native panic wrapper, and unexpected native errors.

Java argument validation still uses standard `NullPointerException`,
`IllegalArgumentException`, or `IllegalStateException` where appropriate.
