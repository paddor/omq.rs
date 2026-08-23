import { Buffer } from "node:buffer";
import { createRequire } from "node:module";

type NativeContextOptions = {
  ioThreads?: number;
};

type NativeSocketOptions = {
  identity?: Uint8Array;
  sendHighWaterMark?: number;
  receiveHighWaterMark?: number;
  reconnectInitialDelayMs?: number;
  reconnectMaxDelayMs?: number;
  lingerMs?: number;
  routerMandatory?: boolean;
  conflate?: boolean;
  xpubNodrop?: boolean;
  onMute?: "block" | "dropNewest" | "dropOldest";
  workloadProfile?: "throughput" | "latency";
  compressionDictionary?: Uint8Array;
  plain?: {
    username: string;
    password: string;
    server?: boolean;
  };
  curve?: {
    serverKey?: string;
    publicKey: string;
    secretKey: string;
    server?: boolean;
  };
};

type NativeRawMessage = Uint8Array | Uint8Array[];
type NativePackedMessages = {
  data: Uint8Array;
  partOffsets: Uint32Array;
  partLengths: Uint32Array;
  messageParts: Uint32Array;
};

type NativeContext = {
  socket(socketType: string, options?: NativeSocketOptions): NativeSocket;
  shareKey(): string;
  close(): void;
};

type NativeSocket = {
  bind(endpoint: string): string;
  connect(endpoint: string): void;
  unbind(endpoint: string): void;
  disconnect(endpoint: string): void;
  send(parts: Uint8Array[]): void;
  sendAsync(parts: Uint8Array[]): Promise<void>;
  trySend(parts: Uint8Array[]): boolean;
  trySendBuffer(payload: Buffer): boolean;
  trySendOne(payload: Uint8Array): boolean;
  sendSync(parts: Uint8Array[]): void;
  sendBufferSync(payload: Buffer): void;
  sendOneSync(payload: Uint8Array): void;
  sendGroupSync(group: Uint8Array, payload: Uint8Array): void;
  recv(): Uint8Array[];
  recvRawSync(): Uint8Array | Uint8Array[];
  recvRaw(cancelId?: number): Promise<Uint8Array | Uint8Array[]>;
  cancelRecv(cancelId: number): void;
  recvSync(): Uint8Array[];
  recvTimeout(timeoutMs: number): Uint8Array[] | null;
  tryRecv(): Uint8Array[] | null;
  tryRecvRaw(): Uint8Array | Uint8Array[] | null;
  recvRawManySync(max: number): NativeRawMessage[];
  tryRecvRawManySync(max: number): NativeRawMessage[];
  recvPackedManySync(max: number): NativePackedMessages;
  tryRecvPackedManySync(max: number): NativePackedMessages;
  waitConnectedSync(minPeers: number, timeoutMs: number): number;
  recvManySync(max: number, timeoutMs?: number): Uint8Array[][];
  subscribe(prefix: Uint8Array): void;
  unsubscribe(prefix: Uint8Array): void;
  join(group: Uint8Array): void;
  leave(group: Uint8Array): void;
  close(): void;
};

type NativeModule = {
  NativeContext: new (options?: NativeContextOptions) => NativeContext;
  nativeContextFromShareKey(shareKey: string): NativeContext;
  curveKeypair(): CurveKeypair;
  curvePublic(secretKey: string): string;
};

const native = loadNative();
const RECV_PREFETCH = 64;

/** Bytes or text accepted as one OMQ message frame. */
export type MessagePart = string | ArrayBuffer | Uint8Array | Buffer;

/** OMQ socket type name. */
export type SocketTypeName =
  | "REQ"
  | "REP"
  | "PUB"
  | "SUB"
  | "XPUB"
  | "XSUB"
  | "PUSH"
  | "PULL"
  | "DEALER"
  | "ROUTER"
  | "PAIR"
  | "CLIENT"
  | "SERVER"
  | "RADIO"
  | "DISH"
  | "SCATTER"
  | "GATHER"
  | "CHANNEL"
  | "PEER"
  | "STREAM";

/** Options for an OMQ context. */
export interface ContextOptions {
  /** Number of I/O threads owned by the context. Defaults to 1. */
  ioThreads?: number;
}

/** Options applied when a socket is created. */
export interface SocketOptions {
  /** Socket identity used by ROUTER/DEALER-style routing. */
  identity?: MessagePart;
  /** Create a private context with this many I/O threads when no context is passed. */
  ioThreads?: number;
  /** Outbound high-water mark in messages. */
  sendHighWaterMark?: number;
  /** Inbound high-water mark in messages. */
  receiveHighWaterMark?: number;
  /** Initial reconnect delay in milliseconds. */
  reconnectInitialDelayMs?: number;
  /** Maximum reconnect delay in milliseconds. */
  reconnectMaxDelayMs?: number;
  /** Linger period in milliseconds; negative means forever. */
  lingerMs?: number;
  /** ROUTER sends fail when no route exists. */
  routerMandatory?: boolean;
  /** Keep only the newest message in inbound/outbound queues. */
  conflate?: boolean;
  /** XPUB send fails instead of dropping when subscribers are muted. */
  xpubNodrop?: boolean;
  /** Behavior when a socket cannot currently send. */
  onMute?: "block" | "dropNewest" | "dropOldest";
  /** Workload hint used by the native transport. */
  workloadProfile?: "throughput" | "latency";
  /** Enable LZ4 transport compression, optionally with a dictionary. */
  lz4?: boolean | {
    /** Static LZ4 dictionary bytes shared with peers. */
    dictionary?: MessagePart;
  };
  /** PLAIN authentication options. */
  plain?: {
    /** User name sent by clients or accepted by servers. */
    username: string;
    /** Password sent by clients or accepted by servers. */
    password: string;
    /** Whether this socket acts as a PLAIN server. */
    server?: boolean;
  };
  /** CURVE authentication options. */
  curve?: {
    /** Server public key required by CURVE clients. */
    serverKey?: string;
    /** Public key for this socket. */
    publicKey: string;
    /** Secret key for this socket. */
    secretKey: string;
    /** Whether this socket acts as a CURVE server. */
    server?: boolean;
  };
}

/** CURVE public/secret key pair encoded as Z85 strings. */
export interface CurveKeypair {
  /** Public key. */
  publicKey: string;
  /** Secret key. */
  secretKey: string;
}

/** Receive options. */
export interface RecvOptions {
  /** Abort signal used to cancel an async receive wait. */
  signal?: AbortSignal;
}

/** Immutable OMQ message wrapper with one or more frames. */
export class Message {
  private materializedParts?: Uint8Array[];
  private singlePart?: Uint8Array;
  private packedData?: Uint8Array;
  private packedOffset?: number;
  private packedLength?: number;

  /** Create a message from one frame or a multipart frame array. */
  constructor(input: MessagePart | MessagePart[] = new Uint8Array()) {
    const parts = Array.isArray(input) ? input : [input];
    this.materializedParts = parts.map(toBytes);
  }

  /** Return input unchanged when already a message, otherwise wrap it. */
  static from(input: Message | MessagePart | MessagePart[]): Message {
    return input instanceof Message ? input : new Message(input);
  }

  /** Message frames as byte arrays. */
  get parts(): Uint8Array[] {
    return this.materializeParts();
  }

  /** Number of frames in the message. */
  get length(): number {
    return this.materializedParts?.length ?? (this.singlePart !== undefined || this.packedData !== undefined ? 1 : 0);
  }

  /** Return one frame by index. */
  part(index = 0): Uint8Array {
    if (this.materializedParts === undefined && index !== 0) {
      throw new RangeError(`message part ${index} out of range`);
    }
    if (this.materializedParts === undefined && this.singlePart !== undefined) {
      return this.singlePart;
    }
    if (
      this.materializedParts === undefined &&
      this.packedData !== undefined &&
      this.packedOffset !== undefined &&
      this.packedLength !== undefined
    ) {
      const part = this.packedData.subarray(this.packedOffset, this.packedOffset + this.packedLength);
      this.singlePart = part;
      this.packedData = undefined;
      this.packedOffset = undefined;
      this.packedLength = undefined;
      return part;
    }
    const part = this.materializeParts()[index];
    if (part === undefined) {
      throw new RangeError(`message part ${index} out of range`);
    }
    return part;
  }

  /** Decode one frame as text. */
  string(index = 0, encoding: BufferEncoding = "utf8"): string {
    const part = this.part(index);
    return Buffer.from(part.buffer, part.byteOffset, part.byteLength).toString(encoding);
  }

  /** Return a shallow copy of the frame array. */
  toArray(): Uint8Array[] {
    return this.parts.slice();
  }

  /** Iterate over message frames. */
  [Symbol.iterator](): Iterator<Uint8Array> {
    return this.parts[Symbol.iterator]();
  }

  private materializeParts(): Uint8Array[] {
    if (this.materializedParts !== undefined) {
      return this.materializedParts;
    }
    if (this.singlePart !== undefined) {
      this.materializedParts = [this.singlePart];
      return this.materializedParts;
    }
    if (this.packedData !== undefined && this.packedOffset !== undefined && this.packedLength !== undefined) {
      this.materializedParts = [this.packedData.subarray(this.packedOffset, this.packedOffset + this.packedLength)];
      this.packedData = undefined;
      this.packedOffset = undefined;
      this.packedLength = undefined;
      return this.materializedParts;
    }
    this.materializedParts = [];
    return this.materializedParts;
  }
}

/** Generate a new CURVE key pair. */
export function curveKeypair(): CurveKeypair {
  return native.curveKeypair();
}

/** Derive a CURVE public key from a Z85 secret key. */
export function curvePublic(secretKey: string): string {
  return native.curvePublic(secretKey);
}

/** OMQ context that owns transport runtimes and inproc namespace. */
export class Context {
  readonly #native: NativeContext;
  #closed = false;

  /** Create a context with optional I/O thread configuration. */
  constructor(options?: ContextOptions);
  constructor(options: ContextOptions = {}, nativeContext?: NativeContext) {
    this.#native = nativeContext ?? new native.NativeContext(options);
  }

  /** Recreate a JavaScript context wrapper for an existing native context. */
  static fromShareKey(shareKey: string): Context {
    const construct = Context as unknown as {
      new (options: ContextOptions, nativeContext: NativeContext): Context;
    };
    return new construct({}, native.nativeContextFromShareKey(shareKey));
  }

  /** Create a socket on this context. */
  socket(socketType: SocketTypeName, options: SocketOptions = {}): Socket {
    return new Socket(socketType, options, this);
  }

  /** Close this context and terminate its owned native runtime. */
  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#native.close();
  }

  /** Close this context when used with JavaScript explicit resource management. */
  [Symbol.dispose](): void {
    this.close();
  }

  /** Return the native share key used for inproc sharing. */
  shareKey(): string {
    if (this.#closed) {
      throw new Error("context closed");
    }
    return this.#native.shareKey();
  }

  /** @internal Create a native socket for the high-level Socket wrapper. */
  private _socket(socketType: SocketTypeName, options: SocketOptions): NativeSocket {
    if (this.#closed) {
      throw new Error("context closed");
    }
    return this.#native.socket(socketType, normalizeOptions(options));
  }
}

/** Base class for OMQ sockets. */
export class Socket {
  /** Socket type name. */
  readonly type: SocketTypeName;
  private readonly native: NativeSocket;
  readonly #context: Context;
  readonly #closeContextOnClose: boolean;
  readonly #recvPrefetch: number;
  #recvQueue: Message[] = [];
  #recvQueueOffset = 0;
  #nextRecvCancelId = 1;
  #closed = false;

  /** Create a socket of the given type. Prefer concrete subclasses for normal use. */
  constructor(socketType: SocketTypeName, options: SocketOptions = {}, context?: Context) {
    const contextRef = context ?? defaultContext(options);
    this.type = socketType;
    this.#context = contextRef;
    this.#closeContextOnClose = context === undefined && options.ioThreads !== undefined;
    this.#recvPrefetch = recvPrefetchFor(socketType);
    this.native = contextSocket(contextRef, socketType, options);
  }

  /** Bind the socket and resolve with the concrete endpoint. */
  bind(endpoint: string): Promise<string> {
    this.#checkOpen();
    return callAsPromise(() => this.native.bind(endpoint));
  }

  /** Connect the socket to an endpoint. */
  connect(endpoint: string): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.connect(endpoint));
  }

  /** Stop listening on a bound endpoint. */
  unbind(endpoint: string): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.unbind(endpoint));
  }

  /** Disconnect from a connected endpoint. */
  disconnect(endpoint: string): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.disconnect(endpoint));
  }

  /** Send one message and resolve when accepted by the socket. */
  send(message: Message | MessagePart | MessagePart[]): Promise<void> {
    this.#checkOpen();
    const pending = trySendNative(this.native, message);
    if (pending === null) {
      return Promise.resolve();
    }
    return this.native.sendAsync(pending);
  }

  /** Synchronously send one message. */
  sendSync(message: Message | MessagePart | MessagePart[]): void {
    this.#checkOpen();
    sendNativeSync(this.native, message);
  }

  /** Receive one message, optionally aborting while waiting. */
  async recv(options: RecvOptions = {}): Promise<Message> {
    this.#checkOpen();
    if (options.signal) throwIfAborted(options.signal);
    const raw = this.#tryRecvRaw();
    if (raw !== null) {
      return raw;
    }
    if (options.signal) throwIfAborted(options.signal);
    this.#checkOpen();
    const signal = options.signal;
    const cancelId = signal === undefined ? undefined : this.#takeRecvCancelId();
    const abort = cancelId === undefined ? undefined : () => this.native.cancelRecv(cancelId);
    if (abort !== undefined) signal?.addEventListener("abort", abort, { once: true });
    try {
      const pending = this.native.recvRaw(cancelId);
      if (signal?.aborted && cancelId !== undefined) this.native.cancelRecv(cancelId);
      return messageFromNative(await pending);
    } catch (error) {
      if (signal?.aborted) throwAbortError();
      throw error;
    } finally {
      if (abort !== undefined) signal?.removeEventListener("abort", abort);
    }
  }

  /** Synchronously receive one message. */
  recvSync(): Message {
    this.#checkOpen();
    return this.#recvRawSync();
  }

  /** Return one message if available, otherwise null. */
  tryRecv(): Message | null {
    this.#checkOpen();
    const raw = this.#tryRecvRaw();
    return raw;
  }

  /** Wait until at least minPeers are connected, returning connected peer count. */
  waitConnectedSync(minPeers = 1, timeoutMs = 5000): number {
    this.#checkOpen();
    return this.native.waitConnectedSync(minPeers, timeoutMs);
  }

  /** Receive up to max messages synchronously. */
  recvManySync(max: number, timeoutMs?: number): Message[] {
    this.#checkOpen();
    const messages: Message[] = [];
    while (messages.length < max) {
      const raw = this.#takeQueuedRaw();
      if (raw === null) break;
      messages.push(raw);
    }
    if (messages.length < max) {
      const remaining = max - messages.length;
      if (timeoutMs === undefined) {
        messages.push(...messagesFromPacked(this.native.recvPackedManySync(remaining)));
      } else {
        messages.push(...this.native.recvManySync(remaining, timeoutMs).map(messageFromNative));
      }
    }
    return messages;
  }

  /** Close the socket. */
  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#recvQueue = [];
    this.#recvQueueOffset = 0;
    try {
      this.native.close();
    } finally {
      if (this.#closeContextOnClose) {
        this.#context.close();
      }
    }
  }

  /** Close this socket when used with JavaScript explicit resource management. */
  [Symbol.dispose](): void {
    this.close();
  }

  /** Async iterator over received messages until the socket closes. */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (!this.#closed) {
      try {
        yield await this.recv();
      } catch (error) {
        if (this.#closed || isClosedError(error)) {
          return;
        }
        throw error;
      }
    }
  }

  #checkOpen(): void {
    if (this.#closed) {
      throw new Error("socket closed");
    }
  }

  #takeRecvCancelId(): number {
    const id = this.#nextRecvCancelId;
    this.#nextRecvCancelId = id === 0xffff_ffff ? 1 : id + 1;
    return id;
  }

  #recvRawSync(): Message {
    const queued = this.#takeQueuedRaw();
    if (queued !== null) return queued;
    if (this.#recvPrefetch <= 1) return messageFromNative(this.native.recvRawSync());
    this.#recvQueue = messagesFromPacked(this.native.recvPackedManySync(this.#recvPrefetch));
    this.#recvQueueOffset = 0;
    return this.#takeQueuedRaw() ?? messageFromNative(this.native.recvRawSync());
  }

  #tryRecvRaw(): Message | null {
    const queued = this.#takeQueuedRaw();
    if (queued !== null) return queued;
    if (this.#recvPrefetch <= 1) {
      const raw = this.native.tryRecvRaw();
      return raw === null ? null : messageFromNative(raw);
    }
    this.#recvQueue = messagesFromPacked(this.native.tryRecvPackedManySync(this.#recvPrefetch));
    this.#recvQueueOffset = 0;
    return this.#takeQueuedRaw();
  }

  #takeQueuedRaw(): Message | null {
    if (this.#recvQueueOffset >= this.#recvQueue.length) {
      this.#recvQueue = [];
      this.#recvQueueOffset = 0;
      return null;
    }
    return this.#recvQueue[this.#recvQueueOffset++];
  }

  /** Subscribe a SUB/XSUB socket to a prefix. */
  protected subscribeNative(prefix: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.subscribe(toBytes(prefix)));
  }

  /** Remove a SUB/XSUB prefix subscription. */
  protected unsubscribeNative(prefix: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.unsubscribe(toBytes(prefix)));
  }

  /** Join a RADIO/DISH group. */
  protected joinNative(group: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.join(toBytes(group)));
  }

  /** Leave a RADIO/DISH group. */
  protected leaveNative(group: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.leave(toBytes(group)));
  }

  /** Send one body to a RADIO group without creating a parts array. */
  protected sendGroupNative(group: MessagePart, body: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.sendGroupSync(toBytes(group), toBytes(body)));
  }
}

/** Strict request socket. Send and receive must alternate. */
export class Req extends Socket {
  /** Create a REQ socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("REQ", options, context);
  }
}

/** Strict reply socket. Receive and send must alternate. */
export class Rep extends Socket {
  /** Create a REP socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("REP", options, context);
  }
}

/** Publisher socket that fans messages out to subscribers. */
export class Pub extends Socket {
  /** Create a PUB socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("PUB", options, context);
  }
}

/** Subscriber socket with prefix subscriptions. */
export class Sub extends Socket {
  /** Create a SUB socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("SUB", options, context);
  }

  /** Subscribe to messages whose first frame starts with prefix. */
  subscribe(prefix: MessagePart): Promise<void> {
    return this.subscribeNative(prefix);
  }

  /** Remove a prefix subscription. */
  unsubscribe(prefix: MessagePart): Promise<void> {
    return this.unsubscribeNative(prefix);
  }
}

/** Raw publisher side of an XPUB/XSUB proxy. */
export class XPub extends Socket {
  /** Create an XPUB socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("XPUB", options, context);
  }
}

/** Raw subscriber side of an XPUB/XSUB proxy. */
export class XSub extends Socket {
  /** Create an XSUB socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("XSUB", options, context);
  }
}

/** Pipeline sender socket. */
export class Push extends Socket {
  /** Create a PUSH socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("PUSH", options, context);
  }
}

/** Pipeline receiver socket. */
export class Pull extends Socket {
  /** Create a PULL socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("PULL", options, context);
  }
}

/** Async request socket without REQ send/receive alternation. */
export class Dealer extends Socket {
  /** Create a DEALER socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("DEALER", options, context);
  }
}

/** Async reply router socket that exposes routing identities. */
export class Router extends Socket {
  /** Create a ROUTER socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("ROUTER", options, context);
  }
}

/** Exclusive bidirectional socket. */
export class Pair extends Socket {
  /** Create a PAIR socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("PAIR", options, context);
  }
}

/** CLIENT socket for single-frame request/reply. */
export class Client extends Socket {
  /** Create a CLIENT socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("CLIENT", options, context);
  }
}

/** SERVER socket for single-frame routed replies. */
export class Server extends Socket {
  /** Create a SERVER socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("SERVER", options, context);
  }
}

/** RADIO group publisher socket. */
export class Radio extends Socket {
  /** Create a RADIO socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("RADIO", options, context);
  }

  /** Send one body to a group. */
  sendGroup(group: MessagePart, body: MessagePart): Promise<void> {
    return this.sendGroupNative(group, body);
  }
}

/** DISH group subscriber socket. */
export class Dish extends Socket {
  /** Create a DISH socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("DISH", options, context);
  }

  /** Join a message group. */
  join(group: MessagePart): Promise<void> {
    return this.joinNative(group);
  }

  /** Leave a message group. */
  leave(group: MessagePart): Promise<void> {
    return this.leaveNative(group);
  }
}

/** Single-frame pipeline sender socket. */
export class Scatter extends Socket {
  /** Create a SCATTER socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("SCATTER", options, context);
  }
}

/** Single-frame pipeline receiver socket. */
export class Gather extends Socket {
  /** Create a GATHER socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("GATHER", options, context);
  }
}

/** Single-frame exclusive bidirectional socket. */
export class Channel extends Socket {
  /** Create a CHANNEL socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("CHANNEL", options, context);
  }
}

/** Bidirectional peer socket with routing identities. */
export class Peer extends Socket {
  /** Create a PEER socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("PEER", options, context);
  }
}

/** Raw TCP stream socket. */
export class Stream extends Socket {
  /** Create a STREAM socket. */
  constructor(options?: SocketOptions, context?: Context) {
    super("STREAM", options, context);
  }
}

let sharedContext: Context | undefined;

function defaultContext(options: SocketOptions): Context {
  if (options.ioThreads !== undefined) {
    return new Context({ ioThreads: options.ioThreads });
  }
  if (sharedContext === undefined) {
    sharedContext = new Context();
  }
  return sharedContext;
}

function contextSocket(context: Context, socketType: SocketTypeName, options: SocketOptions): NativeSocket {
  return (context as unknown as { _socket(socketType: SocketTypeName, options: SocketOptions): NativeSocket })._socket(
    socketType,
    options,
  );
}

function recvPrefetchFor(socketType: SocketTypeName): number {
  switch (socketType) {
    case "PULL":
    case "SUB":
    case "XSUB":
    case "GATHER":
    case "DISH":
      return RECV_PREFETCH;
    default:
      return 1;
  }
}

function callAsPromise<T>(fn: () => T): Promise<T> {
  try {
    return Promise.resolve(fn());
  } catch (error) {
    return Promise.reject(error);
  }
}

function normalizeOptions(options: SocketOptions): NativeSocketOptions {
  return {
    identity: options.identity === undefined ? undefined : toBytes(options.identity),
    sendHighWaterMark: options.sendHighWaterMark,
    receiveHighWaterMark: options.receiveHighWaterMark,
    reconnectInitialDelayMs: options.reconnectInitialDelayMs,
    reconnectMaxDelayMs: options.reconnectMaxDelayMs,
    lingerMs: options.lingerMs,
    routerMandatory: options.routerMandatory,
    conflate: options.conflate,
    xpubNodrop: options.xpubNodrop,
    onMute: options.onMute,
    workloadProfile: options.workloadProfile,
    compressionDictionary:
      typeof options.lz4 === "object" && options.lz4.dictionary !== undefined
        ? toBytes(options.lz4.dictionary)
        : undefined,
    plain: options.plain,
    curve: options.curve,
  };
}

function sendNativeSync(socket: NativeSocket, input: Message | MessagePart | MessagePart[]): void {
  if (input instanceof Message) {
    if (input.length === 1) {
      sendSingleNativeSync(socket, input.part(0));
      return;
    }
    socket.sendSync(input.parts);
    return;
  }

  if (Array.isArray(input)) {
    if (input.length === 1) {
      sendSingleNativeSync(socket, input[0]);
      return;
    }
    socket.sendSync(input.map(toBytes));
    return;
  }

  sendSingleNativeSync(socket, input);
}

function trySendNative(
  socket: NativeSocket,
  input: Message | MessagePart | MessagePart[],
): Uint8Array[] | null {
  if (input instanceof Message) {
    if (input.length === 1) {
      const part = input.part(0);
      return trySendSingleNative(socket, part) ? null : [part];
    }
    const parts = input.parts;
    return socket.trySend(parts) ? null : parts;
  }

  if (Array.isArray(input)) {
    if (input.length === 1) {
      const part = toBytes(input[0]);
      return trySendSingleNative(socket, part) ? null : [part];
    }
    const parts = input.map(toBytes);
    return socket.trySend(parts) ? null : parts;
  }

  const part = toBytes(input);
  return trySendSingleNative(socket, part) ? null : [part];
}

function trySendSingleNative(socket: NativeSocket, input: Uint8Array): boolean {
  if (Buffer.isBuffer(input)) {
    return socket.trySendBuffer(input);
  }
  return socket.trySendOne(input);
}

function sendSingleNativeSync(socket: NativeSocket, input: MessagePart): void {
  if (Buffer.isBuffer(input)) {
    socket.sendBufferSync(input);
    return;
  }
  socket.sendOneSync(toBytes(input));
}

function messageFromNative(nativeMessage: NativeRawMessage): Message {
  const message = Object.create(Message.prototype) as Message;
  if (Array.isArray(nativeMessage)) {
    (message as unknown as { materializedParts?: Uint8Array[] }).materializedParts = nativeMessage;
  } else {
    (message as unknown as { singlePart?: Uint8Array }).singlePart = nativeMessage;
  }
  return message;
}

function messagesFromPacked(batch: NativePackedMessages): Message[] {
  const messages = new Array<Message>(batch.messageParts.length);
  let partIndex = 0;
  for (let messageIndex = 0; messageIndex < batch.messageParts.length; messageIndex++) {
    const result = messageFromPackedAt(batch, messageIndex, partIndex);
    messages[messageIndex] = result.message;
    partIndex = result.nextPartIndex;
  }
  return messages;
}

function messageFromPackedAt(
  batch: NativePackedMessages,
  messageIndex: number,
  partIndex: number,
): { message: Message; nextPartIndex: number } {
  const partCount = batch.messageParts[messageIndex];
  if (partCount === 1) {
    const offset = batch.partOffsets[partIndex];
    const length = batch.partLengths[partIndex];
    const message = Object.create(Message.prototype) as Message;
    (message as unknown as { packedData?: Uint8Array }).packedData = batch.data;
    (message as unknown as { packedOffset?: number }).packedOffset = offset;
    (message as unknown as { packedLength?: number }).packedLength = length;
    return { message, nextPartIndex: partIndex + 1 };
  }

  const parts = new Array<Uint8Array>(partCount);
  for (let index = 0; index < partCount; index++) {
    const offset = batch.partOffsets[partIndex];
    const length = batch.partLengths[partIndex];
    parts[index] = batch.data.subarray(offset, offset + length);
    partIndex++;
  }
  return { message: messageFromNative(parts), nextPartIndex: partIndex };
}

function toBytes(part: MessagePart): Uint8Array {
  if (typeof part === "string") {
    return Buffer.from(part);
  }
  if (Buffer.isBuffer(part)) {
    return part;
  }
  if (part instanceof ArrayBuffer) {
    return new Uint8Array(part);
  }
  return new Uint8Array(part.buffer, part.byteOffset, part.byteLength);
}

function throwIfAborted(signal: AbortSignal): void {
  if (!signal.aborted) {
    return;
  }
  throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}

function throwAbortError(): never {
  throw new DOMException("The operation was aborted", "AbortError");
}

function isClosedError(error: unknown): boolean {
  return error instanceof Error && error.message.toLowerCase().includes("closed");
}

function loadNative(): NativeModule {
  const require = createRequire(__filename);
  try {
    return require("../omq_node.node") as NativeModule;
  } catch (localError) {
    const platformPackage = platformPackageName();
    if (platformPackage !== undefined) {
      try {
        return require(platformPackage) as NativeModule;
      } catch {
        // Fall through to source-build message below.
      }
    }
    const message =
      "Cannot load @paddor/omq-node native addon. Run `npm run build:native` in bindings/node or install a matching prebuild.";
    const error = new Error(message);
    (error as Error & { cause?: unknown }).cause = localError;
    throw error;
  }
}

function platformPackageName(): string | undefined {
  const platform = process.platform;
  const arch = process.arch;
  if (platform === "linux") {
    const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
    const libc = report?.header?.glibcVersionRuntime ? "gnu" : "musl";
    if (arch === "x64") return `@paddor/omq-node-linux-x64-${libc}`;
    if (arch === "arm64") return `@paddor/omq-node-linux-arm64-${libc}`;
  }
  if (platform === "darwin") {
    if (arch === "x64") return "@paddor/omq-node-darwin-x64";
    if (arch === "arm64") return "@paddor/omq-node-darwin-arm64";
  }
  if (platform === "win32") {
    if (arch === "x64") return "@paddor/omq-node-win32-x64-msvc";
    if (arch === "arm64") return "@paddor/omq-node-win32-arm64-msvc";
  }
  return undefined;
}
