# OMQ.java Architecture

Maven binding for `omq-tokio`. The public API targets Java 25. Rust owns the
OMQ context, sockets, protocol state, queues, reconnect logic, auth,
compression, and background I/O.

## Source layout

```
src/main/java/io/omq/
  OMQ.java             static entry points and CURVE helpers
  Context.java         native context handle, socket ownership, cleanup
  Socket.java          synchronized socket facade and option setters
  Message.java         immutable single-part and multipart payloads
  Native.java          JNI declarations and packaged native library loader
  NativeFfm.java       Java 25 FFM downcalls for fast native data paths
  RecvRing.java        off-heap receive cache consumed from Java
  SendRing.java        off-heap single-part send ring produced by Java
  *Exception.java      unchecked exception hierarchy

native/src/
  lib.rs              JNI and C ABI implementation over omq_tokio::blocking
  bin/peer.rs         OMQ.rs peer used by Java interop tests
```

## Threading model

```
Java caller thread -> synchronized Socket -> JNI/FFM -> omq_tokio::blocking::Socket
                                                         |
                                                         v
                                      OMQ Context background I/O thread(s)
                                      connection tasks, queues, ZMTP, auth,
                                      compression, reconnect, transport I/O
```

`Context(int ioThreads)` creates a native `omq_tokio::Context` with that
I/O thread count. Java holds an opaque native handle. Closing an owning Java
context terminates the native context and closes all Java sockets created
from it. `Context.shareKey()` returns a process-local opaque `UUID` view of
the native `u128` context key. `Context.fromShareKey(UUID)` imports another
Java handle to the same native context core; closing the imported handle
closes its Java sockets but does not terminate the owner.

`Socket` holds an opaque native socket handle. The Rust side stores
`Options` and lazily creates the actual `omq_tokio::blocking::Socket` on
first `bind`, `connect`, `send`, `receive`, or wait operation. Option
methods only edit the Rust `Options` value and must run before
materialization.

Java socket methods are `synchronized`, so one Java wrapper cannot race
itself. Native state uses an atomic handle for idempotent close and Rust
mutexes for materialization and option mutation. `Cleaner` is a leak
fallback, not the normal lifecycle path. Use try-with-resources.

## Data path

Java does not encode ZMTP, manage connections, or run transport threads.
Native OMQ receives complete `Message` values and performs all socket
semantics in Rust.

Synchronous receives use a Java 25 FFM fast path. Each Java socket lazily
creates a native off-heap receive ring on first sync receive. The Rust side
fills that ring with `recv_many_into()` using one reused `Vec<Message>`.
Java then drains cached descriptors and payload bytes directly from
`MemorySegment` views. Public methods stay scalar: `receive`,
`receiveBytes`, `receiveInto`, `tryReceive`, and duration variants all use
hidden batches when the ring is empty.

The receive ring mirrors `yring`/`ypipe_t` shape. Rust owns a private producer
cursor. Java owns `head`. Rust publishes a batch by release-storing `tail`;
Java acquires `tail`, reads descriptors with no native call per message, and
release-stores `head` when a cached batch is drained or before refill.
Descriptors and payload storage are native memory. Large payloads that do not
fit the payload ring are held as native external blocks until Java advances
`head`.

Single-part `PUSH` and `SCATTER` sends use a matching FFM send fast path.
Java copies the user `byte[]` into an off-heap SPSC payload ring, writes one
descriptor, and release-stores `tail`. A native worker thread drains
descriptors, builds OMQ `Message` values backed by the off-heap slot, and
submits them to the same native PUSH/SCATTER path used by JNI sends. The slot
is released only when Rust drops the message owner. JNI send paths drain any
queued FFM sends first, preserving call order when applications mix small
single-part sends with multipart, timeout, async, or large-message sends.
Large messages that do not fit the send payload ring wait for the ring to
drain and then use the normal JNI send path. The ring remains usable for
later small messages.

JNI remains the control plane for context/socket setup, options, auth
callbacks, monitors, multipart send, non-`PUSH`/`SCATTER` send, timeout send,
and async completion. The C ABI used by FFM is small and Java-specific; it is
not `omq-libzmq`.

`sendManyBytes` crosses JNI once with a Java `byte[][]` and sends each inner
array as one single-part message. The Rust side copies each array into owned
native message storage before `sendManyBytes` returns; Java buffers are never
retained by native code.

`inproc://` is not special-cased in Java. Each Rust context core owns its
own inproc registry and decides whether an endpoint is local, what socket
types are compatible, and which native queue path is legal. Separate Java
contexts have isolated inproc namespaces. Use `Context.shareKey()` and
`Context.fromShareKey(UUID)` when two Java handles must share one inproc
namespace. A Java-private inproc registry would split address ownership from
Rust and would break mixed Java/Rust endpoints inside the same process.

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

`OMQ.receiveAny(Socket...)` requires distinct sockets and runs one native
task that polls nonblocking receives across them. It returns the first
`ReceiveEvent` it consumes. It never starts loser `recv()` futures, so
messages on other sockets remain queued. Canceling the Java future aborts
the native poll task.
Before spawning native receives, async receive APIs first drain any message
already cached in the Java FFM receive ring. This preserves receive ordering
when sync and async APIs are mixed.

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

## Tests

Normal CI runs the JUnit suite with the native library built by Maven. The
soak test is opt-in and skipped unless `OMQ_JAVA_SOAK=1` or
`-Domq.java.soak=true` is set. `bindings/java/scripts/soak.sh` runs the same
mixed workload at 5, 10, 30, and 60 minutes by default. It exercises TCP peer
churn, CURVE-authenticated churn, `lz4+tcp://`, `zstd+tcp://`, and shared
context `inproc://` traffic while checking heap and RSS growth.
