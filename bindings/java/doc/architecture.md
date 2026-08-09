# OMQ.java Architecture

OMQ.java is a Java API over Rust `omq-tokio`.

- Java owns API shape, lifetime wrappers, argument validation, Java exceptions, and Java 25 FFM views of native rings.
- Rust owns contexts, sockets, routing, queues, transports, ZMTP, reconnect, auth, compression, peer metadata, and I/O threads.

## Runtime

```text
Java caller thread
  -> synchronized Socket method
  -> JNI control path or FFM data path
  -> omq_tokio::blocking::Socket
  -> OMQ context runtime thread(s)
  -> connection tasks and transport I/O
```

- `Context` owns a native `omq_tokio::Context`; the native context owns runtime threads, endpoint state, inproc registry, and sockets.
- `Socket` owns one native socket handle; Java methods are `synchronized`, but socket semantics live in Rust.
- Native handles are atomic; close is idempotent. `Cleaner` is only a leak fallback.
- `Context.shareKey()` exposes a process-local opaque `UUID` for the native `u128` context key.
- `Context.fromShareKey(UUID)` imports a non-owning Java handle to the same native context core.
- Rust materializes the real blocking socket lazily on first `bind`, `connect`, `send`, `receive`, async receive, or wait.
- Socket options are pre-materialization setup; protocol or transport setters fail after materialization.

## Source Map

- `src/main/java/io/omq/Context.java`: context ownership and sharing
- `src/main/java/io/omq/Socket.java`: public socket API and lifecycle
- `src/main/java/io/omq/Native.java`: JNI declarations
- `src/main/java/io/omq/NativeFfm.java`: FFM downcalls
- `src/main/java/io/omq/RecvRing.java`, `SendRing.java`: Java ring views
- `native/src/lib.rs`: JNI/FFM bridge to `omq-tokio`

## Native Boundary

| Path | Role |
| --- | --- |
| JNI | lifecycle, endpoints, options, monitors, auth callbacks, multipart send, timeout send, async setup, error translation |
| FFM | scalar sync receive batches, small single-part `PUSH`/`SCATTER` sends |

The FFM ABI is Java-specific. It is not `omq-libzmq`. Java never encodes ZMTP or drives transport readiness.

## Ring Mechanics

The FFM rings are `yring`-style SPSC queues in native memory, but not the exact
libzmq three-shared-pointer layout. Shared state is two padded atomic cursors:
`head` and `tail`. `closed` is send-only. Each side keeps local cached cursors
to avoid rereading the opposite atomic on every message.

Descriptors live in a power-of-two ring. Payload bytes live in a separate
power-of-two arena. The producer writes payload bytes and descriptor, then
release-stores `tail`. The consumer acquire-loads `tail`, drains descriptors,
then release-stores `head` when slots are reusable.

Receive direction is Rust producer / Java consumer. Send direction is Java
producer / Rust consumer. Large payloads that do not fit the arena leave the
ring path and use native owned storage or JNI fallback.

## Receive Path

- Each socket lazily creates a native off-heap receive ring.
- When Java's cache is empty, Rust fills descriptors and payload storage with `recv_many_into()` and reuses one internal `Vec<Message>`.
- Java drains cached descriptors through `MemorySegment` views.
- `receive`, `receiveBytes`, `receiveInto`, timeout variants, and try variants all use this hidden batch path.

## Send Path

- Small single-part `PUSH` and `SCATTER` sends copy bytes into an off-heap SPSC send ring.
- A native worker drains descriptors and submits OMQ messages on the same Rust path as JNI sends.
- Multipart, large, timeout, async, and non-`PUSH`/`SCATTER` sends use JNI.
- JNI sends drain queued FFM sends first, so mixed APIs preserve call order.

## Async Path

- `sendAsync`, `receiveAsync`, and `OMQ.receiveAny` return `CompletableFuture` without Java worker threads.
- JNI creates a global ref, clones the native async socket, and spawns a Rust future on the OMQ runtime.
- Completion attaches the runtime thread to the JVM as a daemon.
- Canceling or externally completing the Java future drops a native abort token.
- Async receive first drains any message already cached in the FFM receive ring.

## Inproc

`inproc://` belongs to the Rust context core. Separate contexts have separate inproc namespaces. Handles imported with `fromShareKey` share the same native context core and therefore the same inproc namespace. Java has no private inproc registry.

## Native Features

Compression, PLAIN, and CURVE are native OMQ features exposed through Java options and endpoint schemes. Rust performs negotiation, key generation, authentication, compression, and peer metadata extraction.
