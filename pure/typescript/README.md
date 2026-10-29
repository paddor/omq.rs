# @paddor/omq

ZMQ client library for browsers over WebSocket. Implements ZMTP 3.x NULL and
PLAIN security over the ZWS 2.0 framing protocol, with optional LZ4 dictionary
compression.

Supports connect-side WebSocket sockets for REQ/REP, PUB/SUB, XPUB/XSUB,
PUSH/PULL, DEALER/ROUTER, PAIR, CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, PEER,
and CHANNEL. Security mechanisms are NULL and PLAIN.

This package is browser/client focused. It does not bind, open `tcp://`,
`ipc://`, or `inproc://` endpoints, or load native code. Server-side JavaScript
that needs native transports should use an OMQ.rs binding such as
`@paddor/omq-node`.

## Install

Published as `@paddor/omq` on [JSR](https://jsr.io/@paddor/omq) and
[npm](https://www.npmjs.com/package/@paddor/omq).

```sh
deno add jsr:@paddor/omq
```

For npm projects:

```sh
npm install @paddor/omq
```

Import from `@paddor/omq` in Deno and npm projects.

## Usage

### Subscribe

```ts
import { Sub } from "@paddor/omq";

const sub = new Sub();
sub.subscribe("market.");
sub.connect("lz4+ws://broker.example.com/sub");

for await (const msg of sub) {
  console.log(msg.string(0), msg.string(1));
}
```

### Request-Reply

```ts
import { Message, Req } from "@paddor/omq";

const req = new Req();
req.connect("wss://broker.example.com/req");

const reply = await req.send(new Message("hello"));
console.log(reply.string(0));
```

### Push

```ts
import { Message, Push } from "@paddor/omq";

const push = new Push();
push.connect("wss://broker.example.com/push");
await push.ready();

await push.send(new Message("event", JSON.stringify({ ts: Date.now() })));
```

### LZ4 Dictionary Compression

For `lz4+ws://` URLs, the socket initializes LZ4 before the WebSocket handshake.
`initLz4()` is available as an optional prewarm step. A pre-shared dictionary
improves compression ratio on small messages:

```ts
import { initLz4, Sub } from "@paddor/omq";

await initLz4(); // optional prewarm

const dict = new Uint8Array([...]); // pre-shared dictionary
const sub = new Sub({ lz4Dict: dict });
sub.subscribe("");
sub.connect("lz4+ws://broker.example.com/sub");
```

TypeScript senders reject individual LZ4 parts at the 1 GiB multi-block
boundary. Split huge payloads at the application layer.

### PLAIN Authentication

Use PLAIN when the WebSocket server requires username/password auth. Prefer
`wss://` because PLAIN itself does not encrypt credentials.

```ts
const push = new Push({
  plain: { username: "alice", password: "secret" },
});
push.connect("wss://broker.example.com/push");
```

### TLS and Client Authentication

Use `wss://` for transport privacy. Browser JavaScript cannot choose or provide
a client certificate/key for `WebSocket`; mTLS has to be handled by the browser
profile, operating system, or a TLS-terminating proxy. It is not a portable
application-level auth mechanism for this library.

For browser clients, prefer `wss://` plus PLAIN with a scoped token or JWT as
the password when the server needs to authenticate the application peer.

CURVE is not implemented in `omq.ts` for now. Over `wss://`, it would mostly
duplicate TLS encryption and add a larger ZMTP crypto handshake surface. CURVE
over `ws://` could make sense later for ZMQ-style key identity without TLS
client-certificate deployment, but it is intentionally out of scope today.

### WebTransport

The WebTransport carrier targets an `omq-tokio` `quic://` listener through
an `https://` WebTransport URL. Once established, the connection carries
ZMTP 3.1 on one ordered stream and liveness records on a second stream;
the same socket classes,
PLAIN authentication, reconnect, and subscription replay apply. OMQ
compression is not available on WebTransport.

```ts
const req = new Req({
  heartbeatIntervalMs: 1000, // liveness PINGs; peer silence closes after the timeout
  heartbeatTimeoutMs: 5000,
});
req.connect("https://broker.example.com:4433/omq");
```

The server certificate must be trusted by the browser. For short-lived
self-signed ECDSA certificates (at most 14 days), pass their SHA-256 hash:

```ts
new Sub({
  webTransport: {
    serverCertificateHashes: [{ algorithm: "sha-256", value: certSha256 }],
  },
});
```

The listener must list the page origin in `QuicOptions::allowed_origins`.
Each connection requests `allowPooling: false` and `requireUnreliable: true`,
so every OMQ peer gets its own HTTP/3 QUIC connection.
The OMQ listener supports both the draft-02 handshake used by current
Chromium and Firefox and draft-16 with Quickie's `RESET_STREAM_AT` extension.

### Robustness Options

Sockets reconnect by default after peer-side close or WebSocket error. Current
subscriptions and group joins replay after reconnect.

```ts
const sub = new Sub({
  reconnectInitialDelayMs: 100,
  reconnectMaxDelayMs: 5000,
  receiveHighWaterMark: 1000,
  sendHighWaterMark: 1000,
  onError: (error) => console.error(error),
});
```

Use `await socket.ready()` when application code needs to wait for at least one
connection to complete the ZMTP handshake. It resolves immediately if a
connection is already ready and remains pending across reconnect attempts.

For PUB/RADIO sockets, `ready()` only means the peer connection completed its
handshake. It does not prove the remote subscription or group command has
arrived. Use XPUB subscription notifications or an application-level ack when
the first published message must not be missed.

Browser `WebSocket` does not expose inbound TCP backpressure. If
`receiveHighWaterMark` is set and the receive queue is full, omq.ts calls
`onError`, closes the affected connection, and reconnects unless reconnects are
disabled. The overflow is reported as connection failure instead of a silent
queue drop.

### Live Interop Tests

Run `npm run test:interop` from this repo to start real `omq-tokio` WebSocket
peers from the local `../omq.rs` checkout and verify NULL, PLAIN, and
`lz4+ws://` traffic. The Rust fixture depends directly on that sibling checkout
so tests exercise transport changes under development, not an older release.

Run `npm run test:browser` for the headless browser interop tests. The
WebTransport spec probes Chromium and Firefox against the fixture's
`quic://` listener, trusting its ten-day certificate by hash. It checks
the full message exchange with both browsers. Set `OMQ_TEST_CHROMIUM` or
`OMQ_TEST_FIREFOX` to test a browser executable outside Playwright's pinned
versions. The WebSocket spec runs in Firefox. It
starts a Rust WebSocket peer, serves a temporary browser bundle, and verifies
the same paths through native browser `WebSocket` and WASM LZ4, including
`wss://`. The harness passes the temporary page origin explicitly to Rust
listeners via `OMQ_TS_BROWSER_ORIGIN`. Production Rust listeners must configure
`options.ws.allowed_origins` for their application origins; the default rejects
browser requests carrying Origin.

Run the opt-in mixed browser soak with:

```sh
OMQ_TS_SOAK_DURATION_SECS=3600 npm run soak
```

It sustains WS/WSS traffic across NULL, PLAIN, and LZ4 sockets while checking
REQ/REP, PUSH/PULL, PUB/SUB, reconnect, connection churn, multipart messages,
large payloads, sequence integrity, and browser errors.
