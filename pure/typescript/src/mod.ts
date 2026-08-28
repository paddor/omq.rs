/**
 * @module
 *
 * ZMQ client library for browsers over WebSocket. Implements ZMTP 3.x NULL
 * and PLAIN security over the ZWS 2.0 framing protocol, with optional LZ4 dictionary
 * compression via {@link https://jsr.io/@paddor/lz4rip | @paddor/lz4rip}.
 *
 * ```ts
 * import { Sub } from "@paddor/omq";
 *
 * const sub = new Sub();
 * sub.subscribe("market.");
 * sub.connect("lz4+ws://broker.example.com/sub");
 *
 * for await (const msg of sub) {
 *   console.log(msg.string(0), msg.string(1));
 * }
 * ```
 */

export { initLz4 } from "./lz4.ts";
export { Message } from "./message.ts";
export { Socket } from "./socket.ts";

export { Req } from "./req.ts";
export { Rep } from "./rep.ts";
export { Pub } from "./pub.ts";
export { Sub } from "./sub.ts";
export { XPub } from "./xpub.ts";
export { XSub } from "./xsub.ts";
export { Push } from "./push.ts";
export { Pull } from "./pull.ts";
export { Dealer } from "./dealer.ts";
export { Router } from "./router.ts";
export { Pair } from "./pair.ts";

export { Client } from "./client.ts";
export { Server } from "./server.ts";
export { Radio } from "./radio.ts";
export { Dish } from "./dish.ts";
export { Gather } from "./gather.ts";
export { Scatter } from "./scatter.ts";
export { Peer } from "./peer.ts";
export { Channel } from "./channel.ts";

export type { SocketOptions } from "./socket.ts";
export type { PlainAuthOptions } from "./auth.ts";
export type { PeerProperties } from "./command.ts";
export type { SocketTypeName } from "./command.ts";
