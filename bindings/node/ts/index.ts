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

type NativeContext = {
  socket(socketType: string, options?: NativeSocketOptions): NativeSocket;
  shareKey(): string;
  close(): void;
};

type NativeSocket = {
  bind(endpoint: string): Promise<string>;
  connect(endpoint: string): Promise<void>;
  unbind(endpoint: string): Promise<void>;
  disconnect(endpoint: string): Promise<void>;
  send(parts: Uint8Array[]): Promise<void>;
  sendSync(parts: Uint8Array[]): void;
  recv(): Promise<Uint8Array[]>;
  recvSync(): Uint8Array[];
  recvTimeout(timeoutMs: number): Promise<Uint8Array[] | null>;
  tryRecv(): Uint8Array[] | null;
  waitConnectedSync(minPeers: number, timeoutMs: number): number;
  recvManySync(max: number, timeoutMs?: number): Uint8Array[][];
  subscribe(prefix: Uint8Array): Promise<void>;
  unsubscribe(prefix: Uint8Array): Promise<void>;
  join(group: Uint8Array): Promise<void>;
  leave(group: Uint8Array): Promise<void>;
  close(): void;
};

type NativeModule = {
  NativeContext: new (options?: NativeContextOptions) => NativeContext;
  nativeContextFromShareKey(shareKey: string): NativeContext;
  curveKeypair(): CurveKeypair;
  curvePublic(secretKey: string): string;
};

const native = loadNative();

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
  readonly parts: Uint8Array[];

  constructor(input: MessagePart | MessagePart[] = new Uint8Array()) {
    const parts = Array.isArray(input) ? input : [input];
    this.parts = parts.map(toBytes);
  }

  static from(input: Message | MessagePart | MessagePart[]): Message {
    return input instanceof Message ? input : new Message(input);
  }

  get length(): number {
    return this.parts.length;
  }

  part(index = 0): Uint8Array {
    const part = this.parts[index];
    if (part === undefined) {
      throw new RangeError(`message part ${index} out of range`);
    }
    return part;
  }

  string(index = 0, encoding: BufferEncoding = "utf8"): string {
    return Buffer.from(this.part(index).buffer, this.part(index).byteOffset, this.part(index).byteLength).toString(encoding);
  }

  toArray(): Uint8Array[] {
    return this.parts.slice();
  }

  [Symbol.iterator](): Iterator<Uint8Array> {
    return this.parts[Symbol.iterator]();
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
  #closed = false;

  constructor(socketType: SocketTypeName, options: SocketOptions = {}, context = defaultContext(options)) {
    this.type = socketType;
    this.native = context._socket(socketType, options);
  }

  bind(endpoint: string): Promise<string> {
    this.#checkOpen();
    return this.native.bind(endpoint);
  }

  connect(endpoint: string): Promise<void> {
    this.#checkOpen();
    return this.native.connect(endpoint);
  }

  unbind(endpoint: string): Promise<void> {
    this.#checkOpen();
    return this.native.unbind(endpoint);
  }

  disconnect(endpoint: string): Promise<void> {
    this.#checkOpen();
    return this.native.disconnect(endpoint);
  }

  send(message: Message | MessagePart | MessagePart[]): Promise<void> {
    this.#checkOpen();
    return this.native.send(partsFrom(message));
  }

  sendSync(message: Message | MessagePart | MessagePart[]): void {
    this.#checkOpen();
    this.native.sendSync(partsFrom(message));
  }

  async recv(options: RecvOptions = {}): Promise<Message> {
    this.#checkOpen();
    if (!options.signal) {
      return new Message(await this.native.recv());
    }
    throwIfAborted(options.signal);
    while (true) {
      const parts = await this.native.recvTimeout(50);
      if (parts !== null) {
        return new Message(parts);
      }
      throwIfAborted(options.signal);
      this.#checkOpen();
    }
  }

  recvSync(): Message {
    this.#checkOpen();
    return new Message(this.native.recvSync());
  }

  tryRecv(): Message | null {
    this.#checkOpen();
    const parts = this.native.tryRecv();
    return parts === null ? null : new Message(parts);
  }

  waitConnectedSync(minPeers = 1, timeoutMs = 5000): number {
    this.#checkOpen();
    return this.native.waitConnectedSync(minPeers, timeoutMs);
  }

  recvManySync(max: number, timeoutMs?: number): Message[] {
    this.#checkOpen();
    return this.native.recvManySync(max, timeoutMs).map((parts) => new Message(parts));
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
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

  protected subscribeNative(prefix: MessagePart): Promise<void> {
    this.#checkOpen();
    return this.native.subscribe(toBytes(prefix));
  }

  protected unsubscribeNative(prefix: MessagePart): Promise<void> {
    this.#checkOpen();
    return this.native.unsubscribe(toBytes(prefix));
  }

  protected joinNative(group: MessagePart): Promise<void> {
    this.#checkOpen();
    return this.native.join(toBytes(group));
  }

  protected leaveNative(group: MessagePart): Promise<void> {
    this.#checkOpen();
    return this.native.leave(toBytes(group));
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

function partsFrom(input: Message | MessagePart | MessagePart[]): Uint8Array[] {
  return Message.from(input).parts;
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
