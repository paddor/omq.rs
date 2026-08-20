# @paddor/omq-node Architecture

`@paddor/omq-node` is a TypeScript/JavaScript API over a NAPI-RS addon
implemented in `bindings/node/src`. The addon uses `omq-tokio`, not libzmq.

- JavaScript owns API shape, typed socket classes, `Message`, option
  normalization, and Promise-facing behavior.
- Rust owns contexts, sockets, routing, queues, transports, ZMTP, reconnect,
  authentication, compression, peer metadata, and I/O threads.
- NAPI-RS owns object binding and generated native loader glue.

## Runtime

```text
JavaScript caller
  -> Socket / Context wrapper
  -> NAPI native method or AsyncTask
  -> omq_tokio::blocking::Socket
  -> OMQ context runtime thread(s)
  -> connection tasks and transport I/O
```

`Context` wraps one native `omq_tokio::Context`. A socket either uses a passed
context or the shared default context. `Context.shareKey()` and
`Context.fromShareKey(...)` expose explicit native context sharing for
`inproc://`; separate contexts keep separate inproc namespaces.

Socket methods are single-thread JavaScript operations. Native state is guarded
by Rust locks and atomic close flags, so close is idempotent and later calls fail
with typed JavaScript errors.

## Source Map

- `ts/index.ts`: public API, typed socket classes, `Message`, batching helpers
- `src/lib.rs`: NAPI addon, native context/socket handles, option conversion
- `index.js`: generated platform loader
- `index.d.ts`, `dist/index.d.ts`: generated TypeScript declarations
- `scripts/update_perf.py`: binding benchmark and chart generator

## Data Path

Outbound strings, buffers, typed arrays, and array buffers are copied into OMQ
messages before native send. Rust never stores JavaScript-owned pointers after a
call returns.

Inbound messages are copied into Node `Uint8Array` values. Single-part receives
return one typed array; multipart receives return arrays of typed arrays.
`recvPackedManySync()` batches many messages into one packed byte array plus
offset tables, and `Message` materializes slices lazily.

The current public API is copy-based. That keeps V8 lifetime and native socket
ownership simple. The batch receive path reduces per-message NAPI overhead but
does not promise zero-copy transport ownership.

## Async and Sync Calls

Most public methods return `Promise`, but many are synchronous native calls
wrapped with `Promise.resolve` / `Promise.reject`. This keeps API shape async
without moving short send/connect calls to worker threads.

`recv({ signal })` first drains cached native messages. If none are ready, it
uses a NAPI `AsyncTask` around a cancelable blocking receive. AbortSignal
cancellation maps to `BlockingRecvCancel`.

Sync APIs exist for hot loops:

- `sendSync`
- `recvSync`
- `tryRecv`
- `recvManySync`
- `waitConnectedSync`

## Socket Features

All OMQ socket types are exposed as typed classes. `SUB` and `XSUB` use
`subscribe` / `unsubscribe`; `RADIO` and `DISH` use group-aware send and
`join` / `leave`.

Compression, PLAIN, and CURVE are native OMQ features exposed through socket
options. JavaScript passes config; Rust performs negotiation, key derivation,
authentication, compression, and peer metadata handling.
