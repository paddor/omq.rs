# OMQ.beam Architecture

OMQ.beam is a three-layer binding:

- Erlang public API in `src/omq.erl`.
- Native Erlang module in `src/omq_nif.erl`.
- Rust NIF implementation in `native/src/lib.rs`, backed by `omq-tokio`.

Elixir and Gleam do not load separate native libraries. Their wrappers call
the Erlang API or an Erlang FFI shim, so Erlang owns the BEAM-facing resource
model for all three languages.

## Contexts

`omq:context/0,1` creates a Rust `omq_tokio::Context` resource. The context
owns the native runtime and IO threads. The BEAM resource keeps the context
alive until explicit `term/1`, `destroy/1`, or resource collection.
`context_instance/0,1` stores a process-wide singleton context in
`persistent_term` and replaces it when the stored context has closed.

Sockets keep the BEAM context resource that created them plus a clone of the
native context. `term/1` marks that wrapper closed and, for owning contexts,
requests native context shutdown. Sockets created from a terminated context
wrapper report closed and reject later materialization.

`share_key/1` returns the native context-core key. `from_share_key/1` imports
that core into another BEAM wrapper without taking runtime ownership. Terming
the imported context only marks that wrapper closed; terming the owner shuts
down the native core and all imported contexts observe it as closed.

## Sockets

`omq:socket/2` creates a lightweight BEAM resource. The native
`omq_tokio::blocking::Socket` is materialized lazily on the first operation
that needs backend state. Options set before materialization are accumulated
in a native `Options` value and applied when the socket is built.

The intended ownership model is one BEAM process per socket. BEAM resources
can be passed between processes, and the native socket handle can serialize
concurrent whole-message operations, but the Erlang wrapper owns some
compatibility state in the calling process dictionary. `SNDMORE` queues partial
send frames per caller process, and `recv_frame/1,2` stores remaining received
frames per caller process for `RCVMORE`. Single-part socket types avoid this
multipart state, but sharing one socket still leaves message ownership and
REQ/REP sequencing to racing callers.

The native socket resource contains:

- monotonically increasing wrapper socket ID
- socket type
- option overlay
- wrapper-only receive and send timeouts
- security option staging for PLAIN and CURVE
- small receive buffer used by `poll/2` and `select/4`
- closed flag
- lazy blocking socket handle

The resource destructor closes the native socket, so abandoned BEAM socket
handles do not leave native socket state alive.

## Calls

Blocking calls that may wait on IO use dirty IO NIF scheduling:

- `bind`
- `connect`
- `unbind`
- `disconnect`
- `send`
- `recv`
- subscription and group control
- close and wait helpers

Nonblocking calls use normal NIF scheduling:

- `try_send`
- `try_recv`
- option getters/setters
- socket type constants and metadata

`send/2,3` and `send_multipart/2,3` convert iodata to binaries in Erlang, then
copy parts into Rust `Bytes` for native submission. `SNDMORE` is buffered in
the calling BEAM process until a final send flushes one native multipart
message. `NOBLOCK` and `DONTWAIT` route through the native `try_send` path.
`send_string/2,3,4` converts UTF-8 Erlang text into the requested wire
encoding before using the same send path. `send_json/2,3` encodes values with
OTP `json`. `send_term/2,3` serializes Erlang terms with external term format
and then uses the normal send path.

`recv/1,2` converts native message parts into BEAM binaries. `recv_frame/1,2`
stores remaining frames in the calling BEAM process so `RCVMORE` can report
frame iteration state. Routing IDs are exposed as Erlang maps for
SERVER/CLIENT and other routing-ID-bearing messages.
`recv_string/1,2,3` and `try_recv_string/1,2` decode one binary frame back to
UTF-8 Erlang text. `recv_json/1,2` and `try_recv_json/1` decode one frame with
OTP `json`. `recv_term/1,2` and `try_recv_term/1` decode one frame with
`binary_to_term/2` in safe mode.

## Monitoring

`monitor/1` materializes the socket and returns a native monitor resource.
The resource owns an `omq_tokio::MonitorStream` behind a mutex.
`monitor_recv/1,2` runs as dirty IO and polls that stream until an event,
timeout, lag notification, or close. `monitor_try_recv/1` is nonblocking.

Monitor events are encoded as Erlang maps with atom keys. Connection snapshots
from `connections/1` and `connection_info/2` use the same map shape for peer
metadata. Elixir receives those maps unchanged. Gleam exposes them as opaque
external types.

## Readiness

`poll/2` and `select/4` are implemented in Erlang over the native
`wait_any/2` helper. `wait_any/2` probes each socket with nonblocking receive.
When a message is found, it is pushed into the socket resource receive buffer.
The later `recv` call drains that buffer before touching the native socket.

`POLLOUT` is treated as ready by the Erlang wrapper. `POLLIN` readiness is
native-probed. `POLLERR` is currently a compatibility constant only.

## Proxy

`proxy/2,3` is implemented in Erlang over `poll/2`, `recv_multipart/2`, and
`send_multipart/3`. It forwards both directions and optionally mirrors each
message to a capture socket. `proxy_steerable/4` adds a control socket that
accepts `PAUSE`, `RESUME`, and `TERMINATE` commands. `device/3` is a
libzmq-compatible alias over `proxy/2`. Routing IDs carried by native messages
are preserved when forwarding.

## Options

Option constants follow libzmq numeric values where applicable. Erlang atoms
are resolved to those constants in `omq.erl`; integer option IDs are accepted
directly.

Most transport and protocol options must be set before lazy materialization.
`RCVTIMEO`, `SNDTIMEO`, `SUBSCRIBE`, and `UNSUBSCRIBE` remain mutable after
materialization because they are wrapper state or native socket commands.
`HWM` is implemented as a compatibility alias that sets both `SNDHWM` and
`RCVHWM` and reads back `SNDHWM`.

`set/3` and `get/2` are aliases over `setsockopt/3` and `getsockopt/2`.
`backend_name/0`, `version/0`, `omq_version/0`, `omq_version_info/0`,
`zmq_version/0`, `zmq_version_info/0`, `strerror/1`, `FORWARDER`, `QUEUE`,
`STREAMER`, `NULL`, `PLAIN`, `CURVE`, and poll constants are wrapper-level
metadata and constants.

Unsupported or read-only options return `{error, badarg, Reason}`. Core OMQ
transport options use IDs outside the libzmq range.

The native library builds PLAIN, CURVE, LZ4, and ZSTD by default. `has/1`
delegates to native compile-time feature detection, so builds with custom
Cargo features report actual capability state. CURVE key helper functions live
in the NIF because they operate on native key types.

## Language Wrappers

Elixir `OMQ` is a direct module wrapper around the Erlang API. It preserves
Erlang return tuples and constants.

Gleam uses `omq_gleam_ffi.erl` to translate Erlang terms into Gleam-friendly
`Result` values. The FFI shim does not own native state; it delegates all
resource lifetime to Erlang.
