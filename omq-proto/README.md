# omq-proto

Sans-I/O ZMTP 3.x core. Codec, message types, routing logic. No async runtime, no I/O.

Backend-agnostic foundation for `omq-tokio`. Use this crate directly
only when building a custom backend or embedding the ZMTP codec into a
non-standard transport.

On 32-bit targets, 64-bit ZMTP/WebSocket/LZ4 wire lengths are accepted only
when they fit platform allocation limits. Practical per-frame/per-message
payloads are below 4 GiB.

## What's inside

| Module | What it does |
|--------|-------------|
| `Connection` | Sans-I/O ZMTP 3.x codec (feed bytes in, pull messages out) |
| `Message` | Zero-copy multi-frame messages, inline up to 55 B |
| Greeting / handshake | ZMTP 3.0/3.1 negotiation and mechanism dispatch |
| Mechanisms | NULL, PLAIN, CURVE |
| Transforms | LZ4 frame-level compression |
| `Endpoint` | Parser for `tcp://`, `ipc://` (Unix sockets / Windows named pipes), `inproc://`, `udp://`, `lz4+tcp://`, `ws://`, `wss://` |
| `SocketType` | 20 types (11 stable, 8 draft, plus STREAM) with compatibility matrix |
| `SubscriptionSet` | Prefix-trie for PUB/SUB topic filtering |
| Monitor types | `MonitorEvent`, `DisconnectReason`, `PeerInfo` |

## Features

All opt-in. Default build needs no C compiler and no crypto deps.

| Feature | Adds | Dependencies |
|---------|------|--------------|
| `plain` | PLAIN mechanism | none |
| `curve` | CURVE mechanism (RFC 26) | `crypto_box`, `crypto_secretbox` |
| `lz4` | LZ4 compression | `lz4rip` |
| `ws` | WebSocket transport | - (backends add `rustls`) |

## License

ISC
