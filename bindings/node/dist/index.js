"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Stream = exports.Peer = exports.Channel = exports.Gather = exports.Scatter = exports.Dish = exports.Radio = exports.Server = exports.Client = exports.Pair = exports.Router = exports.Dealer = exports.Pull = exports.Push = exports.XSub = exports.XPub = exports.Sub = exports.Pub = exports.Rep = exports.Req = exports.Socket = exports.Context = exports.Message = void 0;
exports.curveKeypair = curveKeypair;
exports.curvePublic = curvePublic;
const node_buffer_1 = require("node:buffer");
const node_module_1 = require("node:module");
const native = loadNative();
const RECV_PREFETCH = 64;
/** OMQ message wrapper with one or more frames and optional routing metadata. */
class Message {
    materializedParts;
    singlePart;
    packedData;
    packedOffset;
    packedLength;
    /** Opaque SERVER routing ID. Copy it from a request to its reply. */
    routingId;
    /** Create a message from one frame or a multipart frame array. */
    constructor(input = new Uint8Array()) {
        const parts = Array.isArray(input) ? input : [input];
        this.materializedParts = parts.map(toBytes);
    }
    /** Return input unchanged when already a message, otherwise wrap it. */
    static from(input) {
        return input instanceof Message ? input : new Message(input);
    }
    /** Message frames as byte arrays. */
    get parts() {
        return this.materializeParts();
    }
    /** Number of frames in the message. */
    get length() {
        return this.materializedParts?.length ?? (this.singlePart !== undefined || this.packedData !== undefined ? 1 : 0);
    }
    /** Return one frame by index. */
    part(index = 0) {
        if (this.materializedParts === undefined && index !== 0) {
            throw new RangeError(`message part ${index} out of range`);
        }
        if (this.materializedParts === undefined && this.singlePart !== undefined) {
            return this.singlePart;
        }
        if (this.materializedParts === undefined &&
            this.packedData !== undefined &&
            this.packedOffset !== undefined &&
            this.packedLength !== undefined) {
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
    string(index = 0, encoding = "utf8") {
        const part = this.part(index);
        return node_buffer_1.Buffer.from(part.buffer, part.byteOffset, part.byteLength).toString(encoding);
    }
    /** Return a shallow copy of the frame array. */
    toArray() {
        return this.parts.slice();
    }
    /** Iterate over message frames. */
    [Symbol.iterator]() {
        return this.parts[Symbol.iterator]();
    }
    materializeParts() {
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
exports.Message = Message;
/** Generate a new CURVE key pair. */
function curveKeypair() {
    return native.curveKeypair();
}
/** Derive a CURVE public key from a Z85 secret key. */
function curvePublic(secretKey) {
    return native.curvePublic(secretKey);
}
/** OMQ context that owns transport runtimes and inproc namespace. */
class Context {
    #native;
    #closed = false;
    constructor(options = {}, nativeContext) {
        this.#native = nativeContext ?? new native.NativeContext(options);
    }
    /** Recreate a JavaScript context wrapper for an existing native context. */
    static fromShareKey(shareKey) {
        const construct = Context;
        return new construct({}, native.nativeContextFromShareKey(shareKey));
    }
    /** Create a socket on this context. */
    socket(socketType, options = {}) {
        return new Socket(socketType, options, this);
    }
    /** Close this context and terminate its owned native runtime. */
    close() {
        if (this.#closed) {
            return;
        }
        this.#closed = true;
        this.#native.close();
    }
    /** Close this context when used with JavaScript explicit resource management. */
    [Symbol.dispose]() {
        this.close();
    }
    /** Return the native share key used for inproc sharing. */
    shareKey() {
        if (this.#closed) {
            throw new Error("context closed");
        }
        return this.#native.shareKey();
    }
    /** @internal Create a native socket for the high-level Socket wrapper. */
    _socket(socketType, options) {
        if (this.#closed) {
            throw new Error("context closed");
        }
        return this.#native.socket(socketType, normalizeOptions(options));
    }
}
exports.Context = Context;
/** Base class for OMQ sockets. */
class Socket {
    /** Socket type name. */
    type;
    native;
    #context;
    #closeContextOnClose;
    #recvPrefetch;
    #receivesRoutingId;
    #recvQueue = [];
    #recvQueueOffset = 0;
    #nextRecvCancelId = 1;
    #closed = false;
    /** Create a socket of the given type. Prefer concrete subclasses for normal use. */
    constructor(socketType, options = {}, context) {
        const contextRef = context ?? defaultContext(options);
        this.type = socketType;
        this.#context = contextRef;
        this.#closeContextOnClose = context === undefined && options.ioThreads !== undefined;
        this.#recvPrefetch = recvPrefetchFor(socketType);
        this.#receivesRoutingId = socketType === "SERVER";
        this.native = contextSocket(contextRef, socketType, options);
    }
    /** Bind the socket and resolve with the concrete endpoint. */
    bind(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.bind(endpoint));
    }
    /** Connect the socket to an endpoint. */
    connect(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.connect(endpoint));
    }
    /** Stop listening on a bound endpoint. */
    unbind(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.unbind(endpoint));
    }
    /** Disconnect from a connected endpoint. */
    disconnect(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.disconnect(endpoint));
    }
    /** Send one message and resolve when accepted by the socket. */
    send(message) {
        this.#checkOpen();
        const pending = trySendNative(this.native, message);
        if (pending === null) {
            return Promise.resolve();
        }
        return pending.routingId === undefined
            ? this.native.sendAsync(pending.parts)
            : this.native.sendRoutedAsync(pending.parts, pending.routingId);
    }
    /** Synchronously send one message. */
    sendSync(message) {
        this.#checkOpen();
        sendNativeSync(this.native, message);
    }
    /** Receive one message, optionally aborting while waiting. */
    async recv(options = {}) {
        this.#checkOpen();
        if (options.signal)
            throwIfAborted(options.signal);
        const raw = this.#tryRecvRaw();
        if (raw !== null) {
            return raw;
        }
        if (options.signal)
            throwIfAborted(options.signal);
        this.#checkOpen();
        const signal = options.signal;
        const cancelId = signal === undefined ? undefined : this.#takeRecvCancelId();
        const abort = cancelId === undefined ? undefined : () => this.native.cancelRecv(cancelId);
        if (abort !== undefined)
            signal?.addEventListener("abort", abort, { once: true });
        try {
            const pending = this.#receivesRoutingId
                ? this.native.recvRouted(cancelId).then(messageFromRouted)
                : this.native.recvRaw(cancelId).then(messageFromNative);
            if (signal?.aborted && cancelId !== undefined)
                this.native.cancelRecv(cancelId);
            return await pending;
        }
        catch (error) {
            if (signal?.aborted)
                throwAbortError();
            throw error;
        }
        finally {
            if (abort !== undefined)
                signal?.removeEventListener("abort", abort);
        }
    }
    /** Synchronously receive one message. */
    recvSync() {
        this.#checkOpen();
        return this.#recvRawSync();
    }
    /** Return one message if available, otherwise null. */
    tryRecv() {
        this.#checkOpen();
        const raw = this.#tryRecvRaw();
        return raw;
    }
    /** Wait until at least minPeers are connected, returning connected peer count. */
    waitConnectedSync(minPeers = 1, timeoutMs = 5000) {
        this.#checkOpen();
        return this.native.waitConnectedSync(minPeers, timeoutMs);
    }
    /** Receive up to max messages synchronously. */
    recvManySync(max, timeoutMs) {
        this.#checkOpen();
        if (this.#receivesRoutingId) {
            return this.native.recvRoutedManySync(max, timeoutMs).map(messageFromRouted);
        }
        const messages = [];
        while (messages.length < max) {
            const raw = this.#takeQueuedRaw();
            if (raw === null)
                break;
            messages.push(raw);
        }
        if (messages.length < max) {
            const remaining = max - messages.length;
            if (timeoutMs === undefined) {
                messages.push(...messagesFromPacked(this.native.recvPackedManySync(remaining)));
            }
            else {
                messages.push(...this.native.recvManySync(remaining, timeoutMs).map(messageFromNative));
            }
        }
        return messages;
    }
    /** Close the socket. */
    close() {
        if (this.#closed) {
            return;
        }
        this.#closed = true;
        this.#recvQueue = [];
        this.#recvQueueOffset = 0;
        try {
            this.native.close();
        }
        finally {
            if (this.#closeContextOnClose) {
                this.#context.close();
            }
        }
    }
    /** Close this socket when used with JavaScript explicit resource management. */
    [Symbol.dispose]() {
        this.close();
    }
    /** Async iterator over received messages until the socket closes. */
    async *[Symbol.asyncIterator]() {
        while (!this.#closed) {
            try {
                yield await this.recv();
            }
            catch (error) {
                if (this.#closed || isClosedError(error)) {
                    return;
                }
                throw error;
            }
        }
    }
    #checkOpen() {
        if (this.#closed) {
            throw new Error("socket closed");
        }
    }
    #takeRecvCancelId() {
        const id = this.#nextRecvCancelId;
        this.#nextRecvCancelId = id === 0xffff_ffff ? 1 : id + 1;
        return id;
    }
    #recvRawSync() {
        const queued = this.#takeQueuedRaw();
        if (queued !== null)
            return queued;
        if (this.#receivesRoutingId)
            return messageFromRouted(this.native.recvRoutedSync());
        if (this.#recvPrefetch <= 1)
            return messageFromNative(this.native.recvRawSync());
        this.#recvQueue = messagesFromPacked(this.native.recvPackedManySync(this.#recvPrefetch));
        this.#recvQueueOffset = 0;
        return this.#takeQueuedRaw() ?? messageFromNative(this.native.recvRawSync());
    }
    #tryRecvRaw() {
        const queued = this.#takeQueuedRaw();
        if (queued !== null)
            return queued;
        if (this.#receivesRoutingId) {
            const routed = this.native.tryRecvRouted();
            return routed === null ? null : messageFromRouted(routed);
        }
        if (this.#recvPrefetch <= 1) {
            const raw = this.native.tryRecvRaw();
            return raw === null ? null : messageFromNative(raw);
        }
        this.#recvQueue = messagesFromPacked(this.native.tryRecvPackedManySync(this.#recvPrefetch));
        this.#recvQueueOffset = 0;
        return this.#takeQueuedRaw();
    }
    #takeQueuedRaw() {
        if (this.#recvQueueOffset >= this.#recvQueue.length) {
            this.#recvQueue = [];
            this.#recvQueueOffset = 0;
            return null;
        }
        return this.#recvQueue[this.#recvQueueOffset++];
    }
    /** Subscribe a SUB/XSUB socket to a prefix. */
    subscribeNative(prefix) {
        this.#checkOpen();
        return callAsPromise(() => this.native.subscribe(toBytes(prefix)));
    }
    /** Remove a SUB/XSUB prefix subscription. */
    unsubscribeNative(prefix) {
        this.#checkOpen();
        return callAsPromise(() => this.native.unsubscribe(toBytes(prefix)));
    }
    /** Join a RADIO/DISH group. */
    joinNative(group) {
        this.#checkOpen();
        return callAsPromise(() => this.native.join(toBytes(group)));
    }
    /** Leave a RADIO/DISH group. */
    leaveNative(group) {
        this.#checkOpen();
        return callAsPromise(() => this.native.leave(toBytes(group)));
    }
    /** Send one body to a RADIO group without creating a parts array. */
    sendGroupNative(group, body) {
        this.#checkOpen();
        return callAsPromise(() => this.native.sendGroupSync(toBytes(group), toBytes(body)));
    }
}
exports.Socket = Socket;
/** Strict request socket. Send and receive must alternate. */
class Req extends Socket {
    /** Create a REQ socket. */
    constructor(options, context) {
        super("REQ", options, context);
    }
}
exports.Req = Req;
/** Strict reply socket. Receive and send must alternate. */
class Rep extends Socket {
    /** Create a REP socket. */
    constructor(options, context) {
        super("REP", options, context);
    }
}
exports.Rep = Rep;
/** Publisher socket that fans messages out to subscribers. */
class Pub extends Socket {
    /** Create a PUB socket. */
    constructor(options, context) {
        super("PUB", options, context);
    }
}
exports.Pub = Pub;
/** Subscriber socket with prefix subscriptions. */
class Sub extends Socket {
    /** Create a SUB socket. */
    constructor(options, context) {
        super("SUB", options, context);
    }
    /** Subscribe to messages whose first frame starts with prefix. */
    subscribe(prefix) {
        return this.subscribeNative(prefix);
    }
    /** Remove a prefix subscription. */
    unsubscribe(prefix) {
        return this.unsubscribeNative(prefix);
    }
}
exports.Sub = Sub;
/** Raw publisher side of an XPUB/XSUB proxy. */
class XPub extends Socket {
    /** Create an XPUB socket. */
    constructor(options, context) {
        super("XPUB", options, context);
    }
}
exports.XPub = XPub;
/** Raw subscriber side of an XPUB/XSUB proxy. */
class XSub extends Socket {
    /** Create an XSUB socket. */
    constructor(options, context) {
        super("XSUB", options, context);
    }
}
exports.XSub = XSub;
/** Pipeline sender socket. */
class Push extends Socket {
    /** Create a PUSH socket. */
    constructor(options, context) {
        super("PUSH", options, context);
    }
}
exports.Push = Push;
/** Pipeline receiver socket. */
class Pull extends Socket {
    /** Create a PULL socket. */
    constructor(options, context) {
        super("PULL", options, context);
    }
}
exports.Pull = Pull;
/** Async request socket without REQ send/receive alternation. */
class Dealer extends Socket {
    /** Create a DEALER socket. */
    constructor(options, context) {
        super("DEALER", options, context);
    }
}
exports.Dealer = Dealer;
/** Async reply router socket that exposes routing identities. */
class Router extends Socket {
    /** Create a ROUTER socket. */
    constructor(options, context) {
        super("ROUTER", options, context);
    }
}
exports.Router = Router;
/** Exclusive bidirectional socket. */
class Pair extends Socket {
    /** Create a PAIR socket. */
    constructor(options, context) {
        super("PAIR", options, context);
    }
}
exports.Pair = Pair;
/** CLIENT socket for single-frame request/reply. */
class Client extends Socket {
    /** Create a CLIENT socket. */
    constructor(options, context) {
        super("CLIENT", options, context);
    }
}
exports.Client = Client;
/** SERVER socket for single-frame routed replies. */
class Server extends Socket {
    /** Create a SERVER socket. */
    constructor(options, context) {
        super("SERVER", options, context);
    }
}
exports.Server = Server;
/** RADIO group publisher socket. */
class Radio extends Socket {
    /** Create a RADIO socket. */
    constructor(options, context) {
        super("RADIO", options, context);
    }
    /** Send one body to a group. */
    sendGroup(group, body) {
        return this.sendGroupNative(group, body);
    }
}
exports.Radio = Radio;
/** DISH group subscriber socket. */
class Dish extends Socket {
    /** Create a DISH socket. */
    constructor(options, context) {
        super("DISH", options, context);
    }
    /** Join a message group. */
    join(group) {
        return this.joinNative(group);
    }
    /** Leave a message group. */
    leave(group) {
        return this.leaveNative(group);
    }
}
exports.Dish = Dish;
/** Single-frame pipeline sender socket. */
class Scatter extends Socket {
    /** Create a SCATTER socket. */
    constructor(options, context) {
        super("SCATTER", options, context);
    }
}
exports.Scatter = Scatter;
/** Single-frame pipeline receiver socket. */
class Gather extends Socket {
    /** Create a GATHER socket. */
    constructor(options, context) {
        super("GATHER", options, context);
    }
}
exports.Gather = Gather;
/** Single-frame exclusive bidirectional socket. */
class Channel extends Socket {
    /** Create a CHANNEL socket. */
    constructor(options, context) {
        super("CHANNEL", options, context);
    }
}
exports.Channel = Channel;
/** Bidirectional peer socket with routing identities. */
class Peer extends Socket {
    /** Create a PEER socket. */
    constructor(options, context) {
        super("PEER", options, context);
    }
}
exports.Peer = Peer;
/** Raw TCP stream socket. */
class Stream extends Socket {
    /** Create a STREAM socket. */
    constructor(options, context) {
        super("STREAM", options, context);
    }
}
exports.Stream = Stream;
let sharedContext;
function defaultContext(options) {
    if (options.ioThreads !== undefined) {
        return new Context({ ioThreads: options.ioThreads });
    }
    if (sharedContext === undefined) {
        sharedContext = new Context();
    }
    return sharedContext;
}
function contextSocket(context, socketType, options) {
    return context._socket(socketType, options);
}
function recvPrefetchFor(socketType) {
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
function callAsPromise(fn) {
    try {
        return Promise.resolve(fn());
    }
    catch (error) {
        return Promise.reject(error);
    }
}
function normalizeOptions(options) {
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
        compressionDictionary: lz4Dictionary(options.lz4),
        plain: normalizePlain(options.plain),
        curve: options.curve,
    };
}
function normalizePlain(plain) {
    if (plain === undefined)
        return undefined;
    if (plain.server === true) {
        if (plain.credentials !== undefined) {
            if (plain.username !== undefined || plain.password !== undefined) {
                throw new TypeError("PLAIN server must use credentials or username/password, not both");
            }
            return {
                server: true,
                usernames: plain.credentials.map((credential) => credential.username),
                passwords: plain.credentials.map((credential) => credential.password),
            };
        }
        if (plain.username === undefined || plain.password === undefined) {
            throw new TypeError("PLAIN server requires credentials");
        }
        return {
            server: true,
            usernames: [plain.username],
            passwords: [plain.password],
        };
    }
    if (plain.credentials !== undefined) {
        throw new TypeError("PLAIN credentials allowlist requires server: true");
    }
    if (plain.username === undefined || plain.password === undefined) {
        throw new TypeError("PLAIN client requires username and password");
    }
    return { username: plain.username, password: plain.password };
}
function lz4Dictionary(lz4) {
    if (lz4 === undefined)
        return undefined;
    if (typeof lz4 !== "object" || lz4 === null || lz4.dictionary === undefined) {
        throw new TypeError("LZ4 is enabled by an lz4+tcp:// or lz4+ws:// endpoint; the lz4 option only configures a dictionary");
    }
    return toBytes(lz4.dictionary);
}
function sendNativeSync(socket, input) {
    if (input instanceof Message) {
        if (input.routingId !== undefined) {
            socket.sendRoutedSync(input.parts, checkedRoutingId(input.routingId));
            return;
        }
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
function trySendNative(socket, input) {
    if (input instanceof Message) {
        if (input.routingId !== undefined) {
            const routingId = checkedRoutingId(input.routingId);
            const parts = input.parts;
            return socket.trySendRouted(parts, routingId) ? null : { parts, routingId };
        }
        if (input.length === 1) {
            const part = input.part(0);
            return trySendSingleNative(socket, part) ? null : { parts: [part] };
        }
        const parts = input.parts;
        return socket.trySend(parts) ? null : { parts };
    }
    if (Array.isArray(input)) {
        if (input.length === 1) {
            const part = toBytes(input[0]);
            return trySendSingleNative(socket, part) ? null : { parts: [part] };
        }
        const parts = input.map(toBytes);
        return socket.trySend(parts) ? null : { parts };
    }
    const part = toBytes(input);
    return trySendSingleNative(socket, part) ? null : { parts: [part] };
}
function checkedRoutingId(routingId) {
    if (!Number.isInteger(routingId) || routingId <= 0 || routingId > 0xffff_ffff) {
        throw new RangeError("routingId must be an integer from 1 through 4294967295");
    }
    return routingId;
}
function trySendSingleNative(socket, input) {
    if (node_buffer_1.Buffer.isBuffer(input)) {
        return socket.trySendBuffer(input);
    }
    return socket.trySendOne(input);
}
function sendSingleNativeSync(socket, input) {
    if (node_buffer_1.Buffer.isBuffer(input)) {
        socket.sendBufferSync(input);
        return;
    }
    socket.sendOneSync(toBytes(input));
}
function messageFromNative(nativeMessage) {
    const message = Object.create(Message.prototype);
    if (Array.isArray(nativeMessage)) {
        message.materializedParts = nativeMessage;
    }
    else {
        message.singlePart = nativeMessage;
    }
    return message;
}
function messageFromRouted(nativeMessage) {
    const message = Object.create(Message.prototype);
    message.materializedParts = nativeMessage.parts;
    message.routingId = nativeMessage.routingId;
    return message;
}
function messagesFromPacked(batch) {
    const messages = new Array(batch.messageParts.length);
    let partIndex = 0;
    for (let messageIndex = 0; messageIndex < batch.messageParts.length; messageIndex++) {
        const result = messageFromPackedAt(batch, messageIndex, partIndex);
        messages[messageIndex] = result.message;
        partIndex = result.nextPartIndex;
    }
    return messages;
}
function messageFromPackedAt(batch, messageIndex, partIndex) {
    const partCount = batch.messageParts[messageIndex];
    if (partCount === 1) {
        const offset = batch.partOffsets[partIndex];
        const length = batch.partLengths[partIndex];
        const message = Object.create(Message.prototype);
        message.packedData = batch.data;
        message.packedOffset = offset;
        message.packedLength = length;
        return { message, nextPartIndex: partIndex + 1 };
    }
    const parts = new Array(partCount);
    for (let index = 0; index < partCount; index++) {
        const offset = batch.partOffsets[partIndex];
        const length = batch.partLengths[partIndex];
        parts[index] = batch.data.subarray(offset, offset + length);
        partIndex++;
    }
    return { message: messageFromNative(parts), nextPartIndex: partIndex };
}
function toBytes(part) {
    if (typeof part === "string") {
        return node_buffer_1.Buffer.from(part);
    }
    if (node_buffer_1.Buffer.isBuffer(part)) {
        return part;
    }
    if (part instanceof ArrayBuffer) {
        return new Uint8Array(part);
    }
    return new Uint8Array(part.buffer, part.byteOffset, part.byteLength);
}
function throwIfAborted(signal) {
    if (!signal.aborted) {
        return;
    }
    throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}
function throwAbortError() {
    throw new DOMException("The operation was aborted", "AbortError");
}
function isClosedError(error) {
    return error instanceof Error && error.message.toLowerCase().includes("closed");
}
function loadNative() {
    const require = (0, node_module_1.createRequire)(__filename);
    try {
        return require("../omq_node.node");
    }
    catch (localError) {
        const platformPackage = platformPackageName();
        if (platformPackage !== undefined) {
            try {
                return require(platformPackage);
            }
            catch {
                // Fall through to source-build message below.
            }
        }
        const message = "Cannot load @paddor/omq-node native addon. Run `npm run build:native` in bindings/node or install a matching prebuild.";
        const error = new Error(message);
        error.cause = localError;
        throw error;
    }
}
function platformPackageName() {
    const platform = process.platform;
    const arch = process.arch;
    if (platform === "linux") {
        const report = process.report?.getReport();
        const libc = report?.header?.glibcVersionRuntime ? "gnu" : "musl";
        if (arch === "x64")
            return `@paddor/omq-node-linux-x64-${libc}`;
        if (arch === "arm64")
            return `@paddor/omq-node-linux-arm64-${libc}`;
    }
    if (platform === "darwin") {
        if (arch === "x64")
            return "@paddor/omq-node-darwin-x64";
        if (arch === "arm64")
            return "@paddor/omq-node-darwin-arm64";
    }
    if (platform === "win32") {
        if (arch === "x64")
            return "@paddor/omq-node-win32-x64-msvc";
        if (arch === "arm64")
            return "@paddor/omq-node-win32-arm64-msvc";
    }
    return undefined;
}
