import type { PeerProperties } from "./command.ts";
import type { Message } from "./message.ts";

/** Lifecycle states shared by WebSocket and WebTransport connections. */
export type ConnectionState = "connecting" | "handshaking" | "ready" | "closed";

/**
 * One peer connection as seen by socket classes. `Connection` (ZWS over
 * WebSocket) and `WebTransportConnection` (ZMTP over WebTransport) both
 * implement it; routing, auth, and reconnect logic stay transport-neutral.
 */
export interface PeerConnection {
  /** Original endpoint URL passed to the constructor. */
  readonly url: string;
  /** Current connection lifecycle state. */
  readonly connectionState: ConnectionState;
  /** Whether the ZMTP handshake has completed. */
  readonly isReady: boolean;
  /** Peer READY properties after the ZMTP handshake completes. */
  peerProperties: PeerProperties | null;
  /** Send one application message over this ready connection. */
  send(msg: Message): Promise<void>;
  /** Send a raw ZMTP command payload (name length, name, data). */
  sendCommand(payload: Uint8Array): void;
  /** Close the connection and release its resources. */
  close(): void;
}
