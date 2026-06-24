/**
 * @module
 *
 * ZMQ client library for browsers over WebSocket. Implements ZMTP 3.x NULL
 * security over the ZWS 2.0 framing protocol, with optional LZ4 dictionary
 * compression via {@link https://jsr.io/@paddor/lz4rip | @paddor/lz4rip}.
 *
 * ```ts
 * import { initLz4, Sub } from "@zeromq/omq";
 *
 * await initLz4();
 *
 * const sub = new Sub();
 * sub.subscribe("market.");
 * sub.connect("lz4+wss://broker.example.com/sub");
 *
 * for await (const msg of sub) {
 *   console.log(msg.string(0), msg.string(1));
 * }
 * ```
 */

/** Initialize the LZ4 WASM module. Call once before connecting to `lz4+` URLs. */
export { init as initLz4 } from "@paddor/lz4rip"
export { Message } from "./message.ts"
export { Socket } from "./socket.ts"
export { Req } from "./req.ts"
export { Sub } from "./sub.ts"
export { Push } from "./push.ts"
export type { SocketOptions } from "./socket.ts"
export type { PeerProperties } from "./command.ts"
