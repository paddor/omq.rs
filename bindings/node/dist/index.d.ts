import { Buffer } from "node:buffer";
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
export type MessagePart = string | ArrayBuffer | Uint8Array | Buffer;
export type SocketTypeName = "REQ" | "REP" | "PUB" | "SUB" | "XPUB" | "XSUB" | "PUSH" | "PULL" | "DEALER" | "ROUTER" | "PAIR" | "CLIENT" | "SERVER" | "RADIO" | "DISH" | "SCATTER" | "GATHER" | "CHANNEL" | "PEER" | "STREAM";
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
    lz4?: boolean | {
        dictionary?: MessagePart;
    };
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
}
export interface CurveKeypair {
    publicKey: string;
    secretKey: string;
}
export interface RecvOptions {
    signal?: AbortSignal;
}
export declare class Message {
    readonly parts: Uint8Array[];
    constructor(input?: MessagePart | MessagePart[]);
    static from(input: Message | MessagePart | MessagePart[]): Message;
    get length(): number;
    part(index?: number): Uint8Array;
    string(index?: number, encoding?: BufferEncoding): string;
    toArray(): Uint8Array[];
    [Symbol.iterator](): Iterator<Uint8Array>;
}
export declare function curveKeypair(): CurveKeypair;
export declare function curvePublic(secretKey: string): string;
export declare class Context {
    #private;
    constructor(options?: ContextOptions, nativeContext?: NativeContext);
    static fromShareKey(shareKey: string): Context;
    socket(socketType: SocketTypeName, options?: SocketOptions): Socket;
    close(): void;
    shareKey(): string;
    _socket(socketType: SocketTypeName, options: SocketOptions): NativeSocket;
}
export declare class Socket {
    #private;
    readonly type: SocketTypeName;
    protected readonly native: NativeSocket;
    constructor(socketType: SocketTypeName, options?: SocketOptions, context?: Context);
    bind(endpoint: string): Promise<string>;
    connect(endpoint: string): Promise<void>;
    unbind(endpoint: string): Promise<void>;
    disconnect(endpoint: string): Promise<void>;
    send(message: Message | MessagePart | MessagePart[]): Promise<void>;
    sendSync(message: Message | MessagePart | MessagePart[]): void;
    recv(options?: RecvOptions): Promise<Message>;
    recvSync(): Message;
    tryRecv(): Message | null;
    waitConnectedSync(minPeers?: number, timeoutMs?: number): number;
    recvManySync(max: number, timeoutMs?: number): Message[];
    close(): void;
    [Symbol.asyncIterator](): AsyncIterableIterator<Message>;
    protected subscribeNative(prefix: MessagePart): Promise<void>;
    protected unsubscribeNative(prefix: MessagePart): Promise<void>;
    protected joinNative(group: MessagePart): Promise<void>;
    protected leaveNative(group: MessagePart): Promise<void>;
}
export declare class Req extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Rep extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Pub extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Sub extends Socket {
    constructor(options?: SocketOptions, context?: Context);
    subscribe(prefix: MessagePart): Promise<void>;
    unsubscribe(prefix: MessagePart): Promise<void>;
}
export declare class XPub extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class XSub extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Push extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Pull extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Dealer extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Router extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Pair extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Client extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Server extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Radio extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Dish extends Socket {
    constructor(options?: SocketOptions, context?: Context);
    join(group: MessagePart): Promise<void>;
    leave(group: MessagePart): Promise<void>;
}
export declare class Scatter extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Gather extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Channel extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Peer extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export declare class Stream extends Socket {
    constructor(options?: SocketOptions, context?: Context);
}
export {};
