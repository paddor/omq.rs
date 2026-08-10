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
  sendSync(parts: Uint8Array[]): void;
  sendBufferSync(payload: Buffer): void;
  sendOneSync(payload: Uint8Array): void;
  recv(): Uint8Array[];
  recvRawSync(): Uint8Array | Uint8Array[];
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

export type MessagePart = string | ArrayBuffer | Uint8Array | Buffer;
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

export interface ContextOptions {
  ioThreads?: number;
}

export interface SocketOptions {
  identity?: MessagePart;
  ioThreads?: number;
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
  lz4?: boolean | { dictionary?: MessagePart };
  plain?: { username: string; password: string; server?: boolean };
  curve?: {
    serverKey?: string;
    publicKey: string;
    secretKey: string;
    server?: boolean;
  };
}

export interface CurveKeypair {
  publicKey: string;
  secretKey: string;
}

export interface RecvOptions {
  signal?: AbortSignal;
}

export class Message {
  private materializedParts?: Uint8Array[];
  private singlePart?: Uint8Array;
  private packedData?: Uint8Array;
  private packedOffset?: number;
  private packedLength?: number;

  constructor(input: MessagePart | MessagePart[] = new Uint8Array()) {
    const parts = Array.isArray(input) ? input : [input];
    this.materializedParts = parts.map(toBytes);
  }

  static from(input: Message | MessagePart | MessagePart[]): Message {
    return input instanceof Message ? input : new Message(input);
  }

  get parts(): Uint8Array[] {
    return this.materializeParts();
  }

  get length(): number {
    return this.materializedParts?.length ?? (this.singlePart !== undefined || this.packedData !== undefined ? 1 : 0);
  }

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

  string(index = 0, encoding: BufferEncoding = "utf8"): string {
    const part = this.part(index);
    return Buffer.from(part.buffer, part.byteOffset, part.byteLength).toString(encoding);
  }

  toArray(): Uint8Array[] {
    return this.parts.slice();
  }

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

export function curveKeypair(): CurveKeypair {
  return native.curveKeypair();
}

export function curvePublic(secretKey: string): string {
  return native.curvePublic(secretKey);
}

export class Context {
  readonly #native: NativeContext;
  #closed = false;

  constructor(options: ContextOptions = {}, nativeContext?: NativeContext) {
    this.#native = nativeContext ?? new native.NativeContext(options);
  }

  static fromShareKey(shareKey: string): Context {
    return new Context({}, native.nativeContextFromShareKey(shareKey));
  }

  socket(socketType: SocketTypeName, options: SocketOptions = {}): Socket {
    return new Socket(socketType, options, this);
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#native.close();
  }

  shareKey(): string {
    if (this.#closed) {
      throw new Error("context closed");
    }
    return this.#native.shareKey();
  }

  _socket(socketType: SocketTypeName, options: SocketOptions): NativeSocket {
    if (this.#closed) {
      throw new Error("context closed");
    }
    return this.#native.socket(socketType, normalizeOptions(options));
  }
}

export class Socket {
  readonly type: SocketTypeName;
  protected readonly native: NativeSocket;
  readonly #recvPrefetch: number;
  #recvQueue: Message[] = [];
  #recvQueueOffset = 0;
  #closed = false;

  constructor(socketType: SocketTypeName, options: SocketOptions = {}, context = defaultContext(options)) {
    this.type = socketType;
    this.#recvPrefetch = recvPrefetchFor(socketType);
    this.native = context._socket(socketType, options);
  }

  bind(endpoint: string): Promise<string> {
    this.#checkOpen();
    return callAsPromise(() => this.native.bind(endpoint));
  }

  connect(endpoint: string): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.connect(endpoint));
  }

  unbind(endpoint: string): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.unbind(endpoint));
  }

  disconnect(endpoint: string): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.disconnect(endpoint));
  }

  send(message: Message | MessagePart | MessagePart[]): Promise<void> {
    this.#checkOpen();
    sendNativeSync(this.native, message);
    return Promise.resolve();
  }

  sendSync(message: Message | MessagePart | MessagePart[]): void {
    this.#checkOpen();
    sendNativeSync(this.native, message);
  }

  async recv(options: RecvOptions = {}): Promise<Message> {
    this.#checkOpen();
    if (options.signal) throwIfAborted(options.signal);
    while (true) {
      const raw = this.#tryRecvRaw();
      if (raw !== null) {
        return raw;
      }
      if (options.signal) throwIfAborted(options.signal);
      this.#checkOpen();
      await yieldToEventLoop();
    }
  }

  recvSync(): Message {
    this.#checkOpen();
    return this.#recvRawSync();
  }

  tryRecv(): Message | null {
    this.#checkOpen();
    const raw = this.#tryRecvRaw();
    return raw;
  }

  waitConnectedSync(minPeers = 1, timeoutMs = 5000): number {
    this.#checkOpen();
    return this.native.waitConnectedSync(minPeers, timeoutMs);
  }

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

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#recvQueue = [];
    this.#recvQueueOffset = 0;
    this.native.close();
  }

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

  protected subscribeNative(prefix: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.subscribe(toBytes(prefix)));
  }

  protected unsubscribeNative(prefix: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.unsubscribe(toBytes(prefix)));
  }

  protected joinNative(group: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.join(toBytes(group)));
  }

  protected leaveNative(group: MessagePart): Promise<void> {
    this.#checkOpen();
    return callAsPromise(() => this.native.leave(toBytes(group)));
  }
}

export class Req extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("REQ", options, context);
  }
}

export class Rep extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("REP", options, context);
  }
}

export class Pub extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("PUB", options, context);
  }
}

export class Sub extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("SUB", options, context);
  }

  subscribe(prefix: MessagePart): Promise<void> {
    return this.subscribeNative(prefix);
  }

  unsubscribe(prefix: MessagePart): Promise<void> {
    return this.unsubscribeNative(prefix);
  }
}

export class XPub extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("XPUB", options, context);
  }
}

export class XSub extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("XSUB", options, context);
  }
}

export class Push extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("PUSH", options, context);
  }
}

export class Pull extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("PULL", options, context);
  }
}

export class Dealer extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("DEALER", options, context);
  }
}

export class Router extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("ROUTER", options, context);
  }
}

export class Pair extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("PAIR", options, context);
  }
}

export class Client extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("CLIENT", options, context);
  }
}

export class Server extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("SERVER", options, context);
  }
}

export class Radio extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("RADIO", options, context);
  }
}

export class Dish extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("DISH", options, context);
  }

  join(group: MessagePart): Promise<void> {
    return this.joinNative(group);
  }

  leave(group: MessagePart): Promise<void> {
    return this.leaveNative(group);
  }
}

export class Scatter extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("SCATTER", options, context);
  }
}

export class Gather extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("GATHER", options, context);
  }
}

export class Channel extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("CHANNEL", options, context);
  }
}

export class Peer extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("PEER", options, context);
  }
}

export class Stream extends Socket {
  constructor(options?: SocketOptions, context?: Context) {
    super("STREAM", options, context);
  }
}

let sharedContext: Context | undefined;

function defaultContext(options: SocketOptions): Context {
  if (sharedContext === undefined || options.ioThreads !== undefined) {
    sharedContext = new Context({ ioThreads: options.ioThreads });
  }
  return sharedContext;
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

function isClosedError(error: unknown): boolean {
  return error instanceof Error && error.message.toLowerCase().includes("closed");
}

function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
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
      "Cannot load @zeromq/omq-node native addon. Run `npm run build:native` in bindings/node or install a matching prebuild.";
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
    if (arch === "x64") return `@zeromq/omq-node-linux-x64-${libc}`;
    if (arch === "arm64") return `@zeromq/omq-node-linux-arm64-${libc}`;
  }
  if (platform === "darwin") {
    if (arch === "x64") return "@zeromq/omq-node-darwin-x64";
    if (arch === "arm64") return "@zeromq/omq-node-darwin-arm64";
  }
  if (platform === "win32") {
    if (arch === "x64") return "@zeromq/omq-node-win32-x64-msvc";
    if (arch === "arm64") return "@zeromq/omq-node-win32-arm64-msvc";
  }
  return undefined;
}
