import { Connection, type ConnectionOptions } from "./connection.ts";
import type { SocketTypeName } from "./command.ts";
import type { Message } from "./message.ts";

interface Waiter<T> {
  resolve: (value: T) => void;
  reject: (error: Error) => void;
}

interface EndpointState {
  url: string;
  conn: Connection | null;
  reconnectDelayMs: number;
  reconnectTimer: ReturnType<typeof setTimeout> | null;
  closedByUser: boolean;
}

const DEFAULT_RECONNECT_INITIAL_DELAY_MS = 100;
const DEFAULT_RECONNECT_MAX_DELAY_MS = 5000;

function errorFromUnknown(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function validateNonNegativeInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative integer`);
  }
}

function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
  if (typeof timer === "object" && timer && "unref" in timer) {
    const timerWithUnref = timer as { unref?: () => void };
    timerWithUnref.unref?.();
  }
}

/** Options for creating a socket. */
export interface SocketOptions {
  /** ZMQ identity string. Sent to the peer during handshake. */
  identity?: string;
  /** Pre-shared LZ4 dictionary for `lz4+wss://` connections. */
  lz4Dict?: Uint8Array;
  /** Maximum decompressed message size in bytes. Exceeding this closes the connection. */
  maxMessageSize?: number;
  /** Reconnect after peer-side close or WebSocket error. Defaults to true. */
  reconnect?: boolean;
  /** Initial reconnect delay in milliseconds. Defaults to 100. */
  reconnectInitialDelayMs?: number;
  /** Maximum reconnect delay in milliseconds. Defaults to 5000. */
  reconnectMaxDelayMs?: number;
  /** Maximum queued inbound messages. New inbound messages drop when full. */
  receiveHighWaterMark?: number;
  /** Called when a connection or protocol error occurs. */
  onError?: (error: Error) => void;
}

const encoder = new TextEncoder();

/**
 * Abstract base class for ZMQ socket types. Manages connections, handshakes,
 * and round-robin load balancing.
 */
export abstract class Socket {
  /** @ignore */
  protected abstract readonly socketType: SocketTypeName;
  /** @ignore */
  protected connections: Map<string, Connection> = new Map();
  /** @ignore */
  protected readyConnections: Connection[] = [];
  /** @ignore */
  protected opts: SocketOptions;
  /** @ignore */
  protected identity: Uint8Array;
  /** @ignore */
  protected messageQueue: Message[] = [];
  /** @ignore */
  protected messageWaiters: Array<Waiter<Message>> = [];

  private readyWaiters: Array<Waiter<Connection>> = [];
  private endpoints: Map<string, EndpointState> = new Map();
  private urlsByConnection: Map<Connection, string> = new Map();
  private closed = false;

  /** @ignore */
  constructor(opts: SocketOptions = {}) {
    if (opts.maxMessageSize !== undefined) {
      validateNonNegativeInteger("maxMessageSize", opts.maxMessageSize);
    }
    if (opts.receiveHighWaterMark !== undefined) {
      validateNonNegativeInteger(
        "receiveHighWaterMark",
        opts.receiveHighWaterMark,
      );
    }
    if (opts.reconnectInitialDelayMs !== undefined) {
      validateNonNegativeInteger(
        "reconnectInitialDelayMs",
        opts.reconnectInitialDelayMs,
      );
    }
    if (opts.reconnectMaxDelayMs !== undefined) {
      validateNonNegativeInteger(
        "reconnectMaxDelayMs",
        opts.reconnectMaxDelayMs,
      );
    }
    this.opts = opts;
    this.identity = opts.identity
      ? encoder.encode(opts.identity)
      : new Uint8Array(0);
  }

  /** Connect to a ZMQ endpoint. Use `lz4+wss://` for LZ4-compressed connections. */
  connect(url: string): void {
    if (this.closed) throw new Error("Socket closed");
    if (this.endpoints.has(url)) return;

    const endpoint: EndpointState = {
      url,
      conn: null,
      reconnectDelayMs: this.reconnectInitialDelayMs,
      reconnectTimer: null,
      closedByUser: false,
    };
    this.endpoints.set(url, endpoint);
    try {
      this.openConnection(endpoint);
    } catch (error) {
      this.endpoints.delete(url);
      throw error;
    }
  }

  private openConnection(endpoint: EndpointState): void {
    if (this.closed || endpoint.closedByUser) return;
    const connOpts: ConnectionOptions = {
      socketType: this.socketType,
      identity: this.identity,
      lz4Dict: this.opts.lz4Dict,
      maxMessageSize: this.opts.maxMessageSize,
      onReady: (conn) => this.onConnectionReady(conn),
      onMessage: (conn, msg) => this.onConnectionMessage(conn, msg),
      onCommand: (conn, name, body) => this.onCommand(conn, name, body),
      onClose: (conn) => this.onConnectionClosed(conn),
      onError: (conn, err) => this.onConnectionError(conn, err),
    };

    const conn = new Connection(endpoint.url, connOpts);
    endpoint.conn = conn;
    this.connections.set(endpoint.url, conn);
    this.urlsByConnection.set(conn, endpoint.url);
  }

  /** Disconnect from a previously connected endpoint. */
  disconnect(url: string): void {
    const endpoint = this.endpoints.get(url);
    if (!endpoint) return;

    endpoint.closedByUser = true;
    this.clearReconnectTimer(endpoint);
    this.endpoints.delete(url);

    const conn = endpoint.conn;
    if (conn) {
      this.connections.delete(url);
      this.urlsByConnection.delete(conn);
      this.removeReadyConnection(conn);
      conn.close();
    }

    this.rejectWaitersIfNoEndpoints(new Error("Socket has no connections"));
  }

  /** Close all connections. */
  close(): void {
    if (this.closed) return;

    this.closed = true;
    const error = new Error("Socket closed");
    const conns = [...this.connections.values()];

    for (const endpoint of this.endpoints.values()) {
      endpoint.closedByUser = true;
      this.clearReconnectTimer(endpoint);
    }

    this.endpoints.clear();
    this.connections.clear();
    this.urlsByConnection.clear();
    this.readyConnections.length = 0;

    for (const conn of conns) {
      conn.close();
    }

    this.rejectReadyWaiters(error);
    this.rejectMessageWaiters(error);
    this.onSocketClosed(error);
  }

  /** Number of active connections (including those still handshaking). */
  get connectionCount(): number {
    return this.connections.size;
  }

  /** Number of configured endpoints, including endpoints waiting to reconnect. */
  get endpointCount(): number {
    return this.endpoints.size;
  }

  /** Number of connections that have completed the ZMTP handshake. */
  get readyCount(): number {
    return this.readyConnections.length;
  }

  /** @ignore */
  protected onConnectionReady(conn: Connection): void {
    if (this.closed) return;
    if (!this.readyConnections.includes(conn)) {
      this.readyConnections.push(conn);
    }

    const endpoint = this.endpointFor(conn);
    if (endpoint) {
      endpoint.reconnectDelayMs = this.reconnectInitialDelayMs;
    }

    this.drainReadyWaiters();
  }

  /** @ignore */
  protected abstract onConnectionMessage(conn: Connection, msg: Message): void;

  /** @ignore */
  protected onConnectionClosed(conn: Connection): void {
    this.removeReadyConnection(conn);
    const url = this.urlsByConnection.get(conn);
    if (!url) return;

    this.urlsByConnection.delete(conn);
    const endpoint = this.endpoints.get(url);
    if (endpoint?.conn === conn) {
      endpoint.conn = null;
    }
    if (this.connections.get(url) === conn) {
      this.connections.delete(url);
    }

    if (!endpoint || endpoint.closedByUser || this.closed) {
      this.rejectWaitersIfNoEndpoints(new Error("Socket closed"));
      return;
    }

    if (this.opts.reconnect === false) {
      this.endpoints.delete(url);
      this.rejectWaitersIfNoEndpoints(new Error("Connection closed"));
      return;
    }

    this.scheduleReconnect(endpoint);
  }

  /** @ignore */
  protected onCommand(
    _conn: Connection,
    _name: string,
    _body: Uint8Array,
  ): void {
    // Subclasses can override
  }

  /** @ignore */
  protected onConnectionError(_conn: Connection, err: Error): void {
    this.opts.onError?.(err);
  }

  /** @ignore */
  protected enqueueMessage(msg: Message): void {
    const waiter = this.messageWaiters.shift();
    if (waiter) {
      waiter.resolve(msg);
    } else {
      if (
        this.opts.receiveHighWaterMark !== undefined &&
        this.messageQueue.length >= this.opts.receiveHighWaterMark
      ) {
        return;
      }
      this.messageQueue.push(msg);
    }
  }

  /** @ignore */
  protected dequeueMessage(): Promise<Message> {
    const queued = this.messageQueue.shift();
    if (queued) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error("Socket closed"));
    if (this.endpoints.size === 0) {
      return Promise.reject(new Error("Socket has no connections"));
    }
    return new Promise((resolve, reject) => {
      this.messageWaiters.push({ resolve, reject });
    });
  }

  /** @ignore */
  protected waitForReady(): Promise<Connection> {
    if (this.readyConnections.length > 0) {
      return Promise.resolve(this.readyConnections[0]!);
    }
    if (this.closed) return Promise.reject(new Error("Socket closed"));
    if (this.endpoints.size === 0) {
      return Promise.reject(new Error("Socket has no connections"));
    }
    return new Promise((resolve, reject) => {
      this.readyWaiters.push({ resolve, reject });
    });
  }

  /** @ignore */
  protected pickRoundRobin(): Connection | null {
    if (this.readyConnections.length === 0) return null;
    const conn = this.readyConnections.shift()!;
    this.readyConnections.push(conn);
    return conn;
  }

  /** @ignore */
  protected hasOpenEndpoints(): boolean {
    return !this.closed && this.endpoints.size > 0;
  }

  /** @ignore */
  protected sendOnConnection(conn: Connection, msg: Message): Promise<void> {
    return this.runSynchronously(() => conn.send(msg));
  }

  /** @ignore */
  protected runSynchronously(fn: () => void): Promise<void> {
    try {
      fn();
      return Promise.resolve();
    } catch (error) {
      return Promise.reject(errorFromUnknown(error));
    }
  }

  /** @ignore */
  protected onSocketClosed(_error: Error): void {
    // Subclasses can override
  }

  private get reconnectInitialDelayMs(): number {
    return this.opts.reconnectInitialDelayMs ??
      DEFAULT_RECONNECT_INITIAL_DELAY_MS;
  }

  private get reconnectMaxDelayMs(): number {
    return this.opts.reconnectMaxDelayMs ?? DEFAULT_RECONNECT_MAX_DELAY_MS;
  }

  private scheduleReconnect(endpoint: EndpointState): void {
    if (endpoint.reconnectTimer !== null) return;

    const delayMs = endpoint.reconnectDelayMs;
    endpoint.reconnectDelayMs = Math.min(
      endpoint.reconnectDelayMs * 2,
      this.reconnectMaxDelayMs,
    );

    const timer = setTimeout(() => {
      endpoint.reconnectTimer = null;
      this.openConnection(endpoint);
    }, delayMs);
    unrefTimer(timer);
    endpoint.reconnectTimer = timer;
  }

  private clearReconnectTimer(endpoint: EndpointState): void {
    if (endpoint.reconnectTimer !== null) {
      clearTimeout(endpoint.reconnectTimer);
      endpoint.reconnectTimer = null;
    }
  }

  private endpointFor(conn: Connection): EndpointState | null {
    const url = this.urlsByConnection.get(conn);
    return url ? this.endpoints.get(url) ?? null : null;
  }

  private removeReadyConnection(conn: Connection): void {
    const idx = this.readyConnections.indexOf(conn);
    if (idx >= 0) this.readyConnections.splice(idx, 1);
  }

  private drainReadyWaiters(): void {
    while (this.readyWaiters.length > 0 && this.readyConnections.length > 0) {
      const waiter = this.readyWaiters.shift()!;
      const conn = this.pickRoundRobin() ?? this.readyConnections[0]!;
      waiter.resolve(conn);
    }
  }

  private rejectWaitersIfNoEndpoints(error: Error): void {
    if (this.endpoints.size > 0) return;
    this.rejectReadyWaiters(error);
    this.rejectMessageWaiters(error);
    this.onSocketClosed(error);
  }

  private rejectReadyWaiters(error: Error): void {
    const waiters = this.readyWaiters;
    this.readyWaiters = [];
    for (const waiter of waiters) waiter.reject(error);
  }

  private rejectMessageWaiters(error: Error): void {
    const waiters = this.messageWaiters;
    this.messageWaiters = [];
    for (const waiter of waiters) waiter.reject(error);
  }
}
