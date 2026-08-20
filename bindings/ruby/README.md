# omq-rs

Fast Ruby binding for [OMQ.rs](https://github.com/paddor/omq.rs). No libzmq,
FFI, or broker. Networking runs on OMQ-owned Tokio threads. The Ruby API is
synchronous; its waits cooperate with an installed `Fiber.scheduler`.

MRI 3.3+, MRI 4.0+, and TruffleRuby are supported. TruffleRuby 34 does not
provide Ruby's `Fiber.scheduler` API, so waits block the calling thread there.

## Performance

![Ruby binding benchmark](doc/charts/bindings.svg)

The binding benchmark uses separate Ruby processes over TCP and compares
`omq-rs` with [cztop](https://github.com/paddor/cztop), which calls CZMQ and
libzmq through FFI, and [ffi-rzmq](https://github.com/chuckremes/ffi-rzmq),
which calls libzmq directly through FFI. Install CZMQ and libzmq to include
both baselines.

```sh
ruby -Ilib scripts/update_perf.rb
ruby -Ilib scripts/update_perf.rb --quick
ruby -Ilib scripts/update_perf.rb --chart-only
```

Rows append to `~/.cache/omq-rs/bindings.jsonl`. The generated chart is
`doc/charts/bindings.svg`. `--quick` only runs a smoke benchmark and does not
write results or a chart.

## Install

```sh
gem install omq-rs
```

Source installs require Rust 1.93 or newer.

## Usage

```ruby
require "omq/rs"

pull = OMQ.rs(:pull)
push = OMQ.rs(:push)

endpoint = pull.bind("tcp://127.0.0.1:0")
push.connect(endpoint).wait_for_peer(timeout: 2)

push << "hello"
p pull.recv # => ["hello"]

push.close
pull.close
```

Socket classes are also available directly:

```ruby
push = OMQ::Rust::PUSH.new
pull = OMQ.rs::PULL.new
```

`#send` accepts one frame, multiple arguments, or an Array. `#recv` always
returns an Array of frozen binary Strings. SERVER messages prepend a numeric
routing ID: `[routing_id, body]`. ROUTER, STREAM, and PEER use their normal
identity frame. RADIO/DISH messages use `[group, body]`.

`SERVER#peer_info(routing_id)` returns connection metadata for a live route,
including `:peer_address` and `:peer_identity`, or `nil` for a stale route.

All 20 socket types are available: REQ, REP, PUB, SUB, XPUB, XSUB, PUSH,
PULL, DEALER, ROUTER, PAIR, STREAM, CLIENT, SERVER, RADIO, DISH, SCATTER,
GATHER, CHANNEL, and PEER.

Published gems include PLAIN, CURVE, LZ4, zstd, and WebSocket support. Check
features with `OMQ::Rust.has(:curve)`.

## CURVE

Keys use the standard 40-byte Z85 representation.

```ruby
server_public, server_secret = OMQ::Rust.curve_keypair
client_public, client_secret = OMQ::Rust.curve_keypair

pull = OMQ.rs(
  :pull,
  curve_server: true,
  curve_publickey: server_public,
  curve_secretkey: server_secret,
)
pull.set_curve_auth([client_public])

push = OMQ.rs(
  :push,
  curve_serverkey: server_public,
  curve_publickey: client_public,
  curve_secretkey: client_secret,
)
```

`#set_curve_auth` accepts an Array of allowed public keys, a callable receiving
an `OMQ::Rust::MechanismPeerInfo`, or `nil` to accept every valid CURVE client.
Configure it before the first bind, connect, send, receive, or monitor call.
`OMQ::Rust.curve_public(secret_key)` derives a public key.

PLAIN uses `plain_server: true` on the server and `plain_username` plus
`plain_password` on clients. PLAIN authenticates without encryption; use it
only on trusted transports.

## Compression

Use `lz4+tcp://` or `zstd+tcp://` endpoints on both peers. zstd senders accept
`compression_level`, `compression_dict`, and `compression_auto_train` socket
options.

## Monitoring

`socket.monitor` returns an Enumerable monitor. `#recv(timeout:)` blocks for
the next event; `#recv_nowait` returns an event Hash or `nil`. Event hashes
contain `:event` and event-specific fields such as `:endpoint`,
`:connection_id`, and `:peer_identity`.

## Fiber Schedulers

There is no separate async API and omq-rs does not install a scheduler.
`#send`, `#recv`, and connection waits use Ruby IO readiness. When the caller
installs a `Fiber.scheduler`, such as Async, waits suspend only the current
fiber.

TruffleRuby currently has no `Fiber.scheduler` API. Use threads when concurrent
blocking waits are needed there.

```ruby
require "async"
require "omq/rs"

Async do
  OMQ.rs(:pull) do |pull|
    pull.bind("tcp://127.0.0.1:5555")
    p pull.recv
  end
end
```

## Ractors

Ruby 4 Ractors can create and use omq-rs sockets. Each socket must remain owned
by the Ractor that created it. Ractors can communicate through inproc, IPC, or
TCP endpoints; no preparation call is required.

## Development

```sh
bundle install
bundle exec rake
```

## License

[ISC](LICENSE)
