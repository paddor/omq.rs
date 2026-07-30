# @zeromq/omq

ZMQ client library for browsers over WebSocket. Implements ZMTP 3.x NULL
security over the ZWS 2.0 framing protocol, with optional LZ4 dictionary
compression.

Supports connect-side WebSocket sockets for REQ/REP, PUB/SUB, XPUB/XSUB,
PUSH/PULL, DEALER/ROUTER, PAIR, CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, PEER,
and CHANNEL. Security is currently NULL only.

## Usage

### Subscribe

```ts
import { initLz4, Sub } from "@zeromq/omq";

await initLz4();

const sub = new Sub();
sub.subscribe("market.");
sub.connect("lz4+wss://broker.example.com/sub");

for await (const msg of sub) {
  console.log(msg.string(0), msg.string(1));
}
```

### Request-Reply

```ts
import { Message, Req } from "@zeromq/omq";

const req = new Req();
req.connect("wss://broker.example.com/req");

const reply = await req.send(new Message("hello"));
console.log(reply.string(0));
```

### Push

```ts
import { Message, Push } from "@zeromq/omq";

const push = new Push();
push.connect("wss://broker.example.com/push");

await push.send(new Message("event", JSON.stringify({ ts: Date.now() })));
```

### LZ4 Dictionary Compression

For `lz4+wss://` URLs, call `initLz4()` once before connecting. An optional
pre-shared dictionary improves compression ratio on small messages:

```ts
import { initLz4, Sub } from "@zeromq/omq";

await initLz4();

const dict = new Uint8Array([...]); // pre-shared dictionary
const sub = new Sub({ lz4Dict: dict });
sub.subscribe("");
sub.connect("lz4+wss://broker.example.com/sub");
```

TypeScript senders reject individual LZ4 parts at the 1 GiB multi-block
boundary. Split huge payloads at the application layer.

### Robustness Options

Sockets reconnect by default after peer-side close or WebSocket error. Current
subscriptions and group joins replay after reconnect.

```ts
const sub = new Sub({
  reconnectInitialDelayMs: 100,
  reconnectMaxDelayMs: 5000,
  receiveHighWaterMark: 1000,
  onError: (error) => console.error(error),
});
```
