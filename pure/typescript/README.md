# @zeromq/omq

ZMQ client library for browsers over WebSocket. Implements ZMTP 3.x NULL
security over the ZWS 2.0 framing protocol, with optional LZ4 dictionary
compression.

Supports REQ, SUB, and PUSH socket types.

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
import { Req, Message } from "@zeromq/omq";

const req = new Req();
req.connect("wss://broker.example.com/req");

const reply = await req.send(new Message("hello"));
console.log(reply.string(0));
```

### Push

```ts
import { Push, Message } from "@zeromq/omq";

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
