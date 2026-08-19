import { Buffer } from "node:buffer";
/** Bytes or text accepted as one OMQ message frame. */
export type MessagePart = string | ArrayBuffer | Uint8Array | Buffer;
/** OMQ socket type name. */
export type SocketTypeName = "REQ" | "REP" | "PUB" | "SUB" | "XPUB" | "XSUB" | "PUSH" | "PULL" | "DEALER" | "ROUTER" | "PAIR" | "CLIENT" | "SERVER" | "RADIO" | "DISH" | "SCATTER" | "GATHER" | "CHANNEL" | "PEER" | "STREAM";
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
export declare class Message {
    private materializedParts?;
    private singlePart?;
    private packedData?;
    private packedOffset?;
    private packedLength?;
    /** Create a message from one frame or a multipart frame array. */
    constructor(input?: MessagePart | MessagePart[]);
    /** Return input unchanged when already a message, otherwise wrap it. */
    static from(input: Message | MessagePart | MessagePart[]): Message;
    /** Message frames as byte arrays. */
    get parts(): Uint8Array[];
    /** Number of frames in the message. */
    get length(): number;
    /** Return one frame by index. */
    part(index?: number): Uint8Array;
    /** Decode one frame as text. */
    string(index?: number, encoding?: BufferEncoding): string;
    /** Return a shallow copy of the frame array. */
    toArray(): Uint8Array[];
    /** Iterate over message frames. */
    [Symbol.iterator](): Iterator<Uint8Array>;
    private materializeParts;
}
/** Generate a new CURVE key pair. */
export declare function curveKeypair(): CurveKeypair;
/** Derive a CURVE public key from a Z85 secret key. */
export declare function curvePublic(secretKey: string): string;
/** OMQ context that owns transport runtimes and inproc namespace. */
export declare class Context {
    #private;
    /** Create a context with optional I/O thread configuration. */
    constructor(options?: ContextOptions);
    /** Recreate a JavaScript context wrapper for an existing native context. */
    static fromShareKey(shareKey: string): Context;
    /** Create a socket on this context. */
    socket(socketType: SocketTypeName, options?: SocketOptions): Socket;
    /** Close this context and terminate its owned native runtime. */
    close(): void;
    /** Close this context when used with JavaScript explicit resource management. */
    [Symbol.dispose](): void;
    /** Return the native share key used for inproc sharing. */
    shareKey(): string;
    /** @internal Create a native socket for the high-level Socket wrapper. */
    private _socket;
}
/** Base class for OMQ sockets. */
export declare class Socket {
    #private;
    /** Socket type name. */
    readonly type: SocketTypeName;
    private readonly native;
    /** Create a socket of the given type. Prefer concrete subclasses for normal use. */
    constructor(socketType: SocketTypeName, options?: SocketOptions, context?: Context);
    /** Bind the socket and resolve with the concrete endpoint. */
    bind(endpoint: string): Promise<string>;
    /** Connect the socket to an endpoint. */
    connect(endpoint: string): Promise<void>;
    /** Stop listening on a bound endpoint. */
    unbind(endpoint: string): Promise<void>;
    /** Disconnect from a connected endpoint. */
    disconnect(endpoint: string): Promise<void>;
    /** Send one message and resolve when accepted by the socket. */
    send(message: Message | MessagePart | MessagePart[]): Promise<void>;
    /** Synchronously send one message. */
    sendSync(message: Message | MessagePart | MessagePart[]): void;
    /** Receive one message, optionally aborting while waiting. */
    recv(options?: RecvOptions): Promise<Message>;
    /** Synchronously receive one message. */
    recvSync(): Message;
    /** Return one message if available, otherwise null. */
    tryRecv(): Message | null;
    /** Wait until at least minPeers are connected, returning connected peer count. */
    waitConnectedSync(minPeers?: number, timeoutMs?: number): number;
    /** Receive up to max messages synchronously. */
    recvManySync(max: number, timeoutMs?: number): Message[];
    /** Close the socket. */
    close(): void;
    /** Close this socket when used with JavaScript explicit resource management. */
    [Symbol.dispose](): void;
    /** Async iterator over received messages until the socket closes. */
    [Symbol.asyncIterator](): AsyncIterableIterator<Message>;
    /** Subscribe a SUB/XSUB socket to a prefix. */
    protected subscribeNative(prefix: MessagePart): Promise<void>;
    /** Remove a SUB/XSUB prefix subscription. */
    protected unsubscribeNative(prefix: MessagePart): Promise<void>;
    /** Join a RADIO/DISH group. */
    protected joinNative(group: MessagePart): Promise<void>;
    /** Leave a RADIO/DISH group. */
    protected leaveNative(group: MessagePart): Promise<void>;
    /** Send one body to a RADIO group without creating a parts array. */
    protected sendGroupNative(group: MessagePart, body: MessagePart): Promise<void>;
}
/** Strict request socket. Send and receive must alternate. */
export declare class Req extends Socket {
    /** Create a REQ socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Strict reply socket. Receive and send must alternate. */
export declare class Rep extends Socket {
    /** Create a REP socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Publisher socket that fans messages out to subscribers. */
export declare class Pub extends Socket {
    /** Create a PUB socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Subscriber socket with prefix subscriptions. */
export declare class Sub extends Socket {
    /** Create a SUB socket. */
    constructor(options?: SocketOptions, context?: Context);
    /** Subscribe to messages whose first frame starts with prefix. */
    subscribe(prefix: MessagePart): Promise<void>;
    /** Remove a prefix subscription. */
    unsubscribe(prefix: MessagePart): Promise<void>;
}
/** Raw publisher side of an XPUB/XSUB proxy. */
export declare class XPub extends Socket {
    /** Create an XPUB socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Raw subscriber side of an XPUB/XSUB proxy. */
export declare class XSub extends Socket {
    /** Create an XSUB socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Pipeline sender socket. */
export declare class Push extends Socket {
    /** Create a PUSH socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Pipeline receiver socket. */
export declare class Pull extends Socket {
    /** Create a PULL socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Async request socket without REQ send/receive alternation. */
export declare class Dealer extends Socket {
    /** Create a DEALER socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Async reply router socket that exposes routing identities. */
export declare class Router extends Socket {
    /** Create a ROUTER socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Exclusive bidirectional socket. */
export declare class Pair extends Socket {
    /** Create a PAIR socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** CLIENT socket for single-frame request/reply. */
export declare class Client extends Socket {
    /** Create a CLIENT socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** SERVER socket for single-frame routed replies. */
export declare class Server extends Socket {
    /** Create a SERVER socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** RADIO group publisher socket. */
export declare class Radio extends Socket {
    /** Create a RADIO socket. */
    constructor(options?: SocketOptions, context?: Context);
    /** Send one body to a group. */
    sendGroup(group: MessagePart, body: MessagePart): Promise<void>;
}
/** DISH group subscriber socket. */
export declare class Dish extends Socket {
    /** Create a DISH socket. */
    constructor(options?: SocketOptions, context?: Context);
    /** Join a message group. */
    join(group: MessagePart): Promise<void>;
    /** Leave a message group. */
    leave(group: MessagePart): Promise<void>;
}
/** Single-frame pipeline sender socket. */
export declare class Scatter extends Socket {
    /** Create a SCATTER socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Single-frame pipeline receiver socket. */
export declare class Gather extends Socket {
    /** Create a GATHER socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Single-frame exclusive bidirectional socket. */
export declare class Channel extends Socket {
    /** Create a CHANNEL socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Bidirectional peer socket with routing identities. */
export declare class Peer extends Socket {
    /** Create a PEER socket. */
    constructor(options?: SocketOptions, context?: Context);
}
/** Raw TCP stream socket. */
export declare class Stream extends Socket {
    /** Create a STREAM socket. */
    constructor(options?: SocketOptions, context?: Context);
}
