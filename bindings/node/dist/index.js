"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Stream = exports.Peer = exports.Channel = exports.Gather = exports.Scatter = exports.Dish = exports.Radio = exports.Server = exports.Client = exports.Pair = exports.Router = exports.Dealer = exports.Pull = exports.Push = exports.XSub = exports.XPub = exports.Sub = exports.Pub = exports.Rep = exports.Req = exports.Socket = exports.Context = exports.Message = void 0;
exports.curveKeypair = curveKeypair;
exports.curvePublic = curvePublic;
const node_buffer_1 = require("node:buffer");
const node_module_1 = require("node:module");
const native = loadNative();
const RECV_PREFETCH = 64;
class Message {
    materializedParts;
    singlePart;
    packedData;
    packedOffset;
    packedLength;
    constructor(input = new Uint8Array()) {
        const parts = Array.isArray(input) ? input : [input];
        this.materializedParts = parts.map(toBytes);
    }
    static from(input) {
        return input instanceof Message ? input : new Message(input);
    }
    get parts() {
        return this.materializeParts();
    }
    get length() {
        return this.materializedParts?.length ?? (this.singlePart !== undefined || this.packedData !== undefined ? 1 : 0);
    }
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
    string(index = 0, encoding = "utf8") {
        const part = this.part(index);
        return node_buffer_1.Buffer.from(part.buffer, part.byteOffset, part.byteLength).toString(encoding);
    }
    toArray() {
        return this.parts.slice();
    }
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
function curveKeypair() {
    return native.curveKeypair();
}
function curvePublic(secretKey) {
    return native.curvePublic(secretKey);
}
class Context {
    #native;
    #closed = false;
    constructor(options = {}, nativeContext) {
        this.#native = nativeContext ?? new native.NativeContext(options);
    }
    static fromShareKey(shareKey) {
        return new Context({}, native.nativeContextFromShareKey(shareKey));
    }
    socket(socketType, options = {}) {
        return new Socket(socketType, options, this);
    }
    close() {
        if (this.#closed) {
            return;
        }
        this.#closed = true;
        this.#native.close();
    }
    shareKey() {
        if (this.#closed) {
            throw new Error("context closed");
        }
        return this.#native.shareKey();
    }
    _socket(socketType, options) {
        if (this.#closed) {
            throw new Error("context closed");
        }
        return this.#native.socket(socketType, normalizeOptions(options));
    }
}
exports.Context = Context;
class Socket {
    type;
    native;
    #recvPrefetch;
    #recvQueue = [];
    #recvQueueOffset = 0;
    #closed = false;
    constructor(socketType, options = {}, context = defaultContext(options)) {
        this.type = socketType;
        this.#recvPrefetch = recvPrefetchFor(socketType);
        this.native = context._socket(socketType, options);
    }
    bind(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.bind(endpoint));
    }
    connect(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.connect(endpoint));
    }
    unbind(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.unbind(endpoint));
    }
    disconnect(endpoint) {
        this.#checkOpen();
        return callAsPromise(() => this.native.disconnect(endpoint));
    }
    send(message) {
        this.#checkOpen();
        sendNativeSync(this.native, message);
        return Promise.resolve();
    }
    sendSync(message) {
        this.#checkOpen();
        sendNativeSync(this.native, message);
    }
    async recv(options = {}) {
        this.#checkOpen();
        if (options.signal)
            throwIfAborted(options.signal);
        while (true) {
            const raw = this.#tryRecvRaw();
            if (raw !== null) {
                return raw;
            }
            if (options.signal)
                throwIfAborted(options.signal);
            this.#checkOpen();
            await yieldToEventLoop();
        }
    }
    recvSync() {
        this.#checkOpen();
        return this.#recvRawSync();
    }
    tryRecv() {
        this.#checkOpen();
        const raw = this.#tryRecvRaw();
        return raw;
    }
    waitConnectedSync(minPeers = 1, timeoutMs = 5000) {
        this.#checkOpen();
        return this.native.waitConnectedSync(minPeers, timeoutMs);
    }
    recvManySync(max, timeoutMs) {
        this.#checkOpen();
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
    close() {
        if (this.#closed) {
            return;
        }
        this.#closed = true;
        this.#recvQueue = [];
        this.#recvQueueOffset = 0;
        this.native.close();
    }
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
    #recvRawSync() {
        const queued = this.#takeQueuedRaw();
        if (queued !== null)
            return queued;
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
    subscribeNative(prefix) {
        this.#checkOpen();
        return callAsPromise(() => this.native.subscribe(toBytes(prefix)));
    }
    unsubscribeNative(prefix) {
        this.#checkOpen();
        return callAsPromise(() => this.native.unsubscribe(toBytes(prefix)));
    }
    joinNative(group) {
        this.#checkOpen();
        return callAsPromise(() => this.native.join(toBytes(group)));
    }
    leaveNative(group) {
        this.#checkOpen();
        return callAsPromise(() => this.native.leave(toBytes(group)));
    }
}
exports.Socket = Socket;
class Req extends Socket {
    constructor(options, context) {
        super("REQ", options, context);
    }
}
exports.Req = Req;
class Rep extends Socket {
    constructor(options, context) {
        super("REP", options, context);
    }
}
exports.Rep = Rep;
class Pub extends Socket {
    constructor(options, context) {
        super("PUB", options, context);
    }
}
exports.Pub = Pub;
class Sub extends Socket {
    constructor(options, context) {
        super("SUB", options, context);
    }
    subscribe(prefix) {
        return this.subscribeNative(prefix);
    }
    unsubscribe(prefix) {
        return this.unsubscribeNative(prefix);
    }
}
exports.Sub = Sub;
class XPub extends Socket {
    constructor(options, context) {
        super("XPUB", options, context);
    }
}
exports.XPub = XPub;
class XSub extends Socket {
    constructor(options, context) {
        super("XSUB", options, context);
    }
}
exports.XSub = XSub;
class Push extends Socket {
    constructor(options, context) {
        super("PUSH", options, context);
    }
}
exports.Push = Push;
class Pull extends Socket {
    constructor(options, context) {
        super("PULL", options, context);
    }
}
exports.Pull = Pull;
class Dealer extends Socket {
    constructor(options, context) {
        super("DEALER", options, context);
    }
}
exports.Dealer = Dealer;
class Router extends Socket {
    constructor(options, context) {
        super("ROUTER", options, context);
    }
}
exports.Router = Router;
class Pair extends Socket {
    constructor(options, context) {
        super("PAIR", options, context);
    }
}
exports.Pair = Pair;
class Client extends Socket {
    constructor(options, context) {
        super("CLIENT", options, context);
    }
}
exports.Client = Client;
class Server extends Socket {
    constructor(options, context) {
        super("SERVER", options, context);
    }
}
exports.Server = Server;
class Radio extends Socket {
    constructor(options, context) {
        super("RADIO", options, context);
    }
}
exports.Radio = Radio;
class Dish extends Socket {
    constructor(options, context) {
        super("DISH", options, context);
    }
    join(group) {
        return this.joinNative(group);
    }
    leave(group) {
        return this.leaveNative(group);
    }
}
exports.Dish = Dish;
class Scatter extends Socket {
    constructor(options, context) {
        super("SCATTER", options, context);
    }
}
exports.Scatter = Scatter;
class Gather extends Socket {
    constructor(options, context) {
        super("GATHER", options, context);
    }
}
exports.Gather = Gather;
class Channel extends Socket {
    constructor(options, context) {
        super("CHANNEL", options, context);
    }
}
exports.Channel = Channel;
class Peer extends Socket {
    constructor(options, context) {
        super("PEER", options, context);
    }
}
exports.Peer = Peer;
class Stream extends Socket {
    constructor(options, context) {
        super("STREAM", options, context);
    }
}
exports.Stream = Stream;
let sharedContext;
function defaultContext(options) {
    if (sharedContext === undefined || options.ioThreads !== undefined) {
        sharedContext = new Context({ ioThreads: options.ioThreads });
    }
    return sharedContext;
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
        compressionDictionary: typeof options.lz4 === "object" && options.lz4.dictionary !== undefined
            ? toBytes(options.lz4.dictionary)
            : undefined,
        plain: options.plain,
        curve: options.curve,
    };
}
function sendNativeSync(socket, input) {
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
function isClosedError(error) {
    return error instanceof Error && error.message.toLowerCase().includes("closed");
}
function yieldToEventLoop() {
    return new Promise((resolve) => setImmediate(resolve));
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
        const message = "Cannot load @zeromq/omq-node native addon. Run `npm run build:native` in bindings/node or install a matching prebuild.";
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
            return `@zeromq/omq-node-linux-x64-${libc}`;
        if (arch === "arm64")
            return `@zeromq/omq-node-linux-arm64-${libc}`;
    }
    if (platform === "darwin") {
        if (arch === "x64")
            return "@zeromq/omq-node-darwin-x64";
        if (arch === "arm64")
            return "@zeromq/omq-node-darwin-arm64";
    }
    if (platform === "win32") {
        if (arch === "x64")
            return "@zeromq/omq-node-win32-x64-msvc";
        if (arch === "arm64")
            return "@zeromq/omq-node-win32-arm64-msvc";
    }
    return undefined;
}
