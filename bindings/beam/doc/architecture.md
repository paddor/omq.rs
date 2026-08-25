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
alive until explicit `term/1` or resource collection.

Sockets store a clone of the native context, so socket resources remain valid
while they are open. `term/1` requests context shutdown; open sockets still
close through their own resource path.

## Sockets

`omq:socket/2` creates a lightweight BEAM resource. The native
`omq_tokio::blocking::Socket` is materialized lazily on the first operation
that needs backend state. Options set before materialization are accumulated
in a native `Options` value and applied when the socket is built.

The native socket resource contains:

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
copy parts into Rust `Bytes` for native submission. `recv/1,2` converts native
message parts into BEAM binaries. Routing IDs are exposed as Erlang maps for
SERVER/CLIENT and other routing-ID-bearing messages.

## Readiness

`poll/2` and `select/4` are implemented in Erlang over the native
`wait_any/2` helper. `wait_any/2` probes each socket with nonblocking receive.
When a message is found, it is pushed into the socket resource receive buffer.
The later `recv` call drains that buffer before touching the native socket.

`POLLOUT` is treated as ready by the Erlang wrapper. `POLLIN` readiness is
native-probed. `POLLERR` is currently a compatibility constant only.

## Options

Option constants follow libzmq numeric values where applicable. Erlang atoms
are resolved to those constants in `omq.erl`; integer option IDs are accepted
directly.

Most transport and protocol options must be set before lazy materialization.
`RCVTIMEO`, `SNDTIMEO`, `SUBSCRIBE`, and `UNSUBSCRIBE` remain mutable after
materialization because they are wrapper state or native socket commands.

Unsupported or read-only options return `{error, badarg, Reason}`. Core OMQ
transport options use IDs outside the libzmq range.

## Language Wrappers

Elixir `OMQ` is a direct module wrapper around the Erlang API. It preserves
Erlang return tuples and constants.

Gleam uses `omq_gleam_ffi.erl` to translate Erlang terms into Gleam-friendly
`Result` values. The FFI shim does not own native state; it delegates all
resource lifetime to Erlang.
