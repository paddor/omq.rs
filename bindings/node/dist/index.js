"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Stream = exports.Peer = exports.Channel = exports.Gather = exports.Scatter = exports.Dish = exports.Radio = exports.Server = exports.Client = exports.Pair = exports.Router = exports.Dealer = exports.Pull = exports.Push = exports.XSub = exports.XPub = exports.Sub = exports.Pub = exports.Rep = exports.Req = exports.Socket = exports.Context = exports.Message = void 0;
exports.curveKeypair = curveKeypair;
exports.curvePublic = curvePublic;
const node_buffer_1 = require("node:buffer");
const node_module_1 = require("node:module");
const native = loadNative();
class Message {
    parts;
    constructor(input = new Uint8Array()) {
        const parts = Array.isArray(input) ? input : [input];
        this.parts = parts.map(toBytes);
    }
    static from(input) {
        return input instanceof Message ? input : new Message(input);
    }
    get length() {
        return this.parts.length;
    }
    part(index = 0) {
        const part = this.parts[index];
        if (part === undefined) {
            throw new RangeError(`message part ${index} out of range`);
        }
        return part;
    }
    string(index = 0, encoding = "utf8") {
        return node_buffer_1.Buffer.from(this.part(index).buffer, this.part(index).byteOffset, this.part(index).byteLength).toString(encoding);
    }
    toArray() {
        return this.parts.slice();
    }
    [Symbol.iterator]() {
        return this.parts[Symbol.iterator]();
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
    #closed = false;
    constructor(socketType, options = {}, context = defaultContext(options)) {
        this.type = socketType;
        this.native = context._socket(socketType, options);
    }
    bind(endpoint) {
        this.#checkOpen();
        return this.native.bind(endpoint);
    }
    connect(endpoint) {
        this.#checkOpen();
        return this.native.connect(endpoint);
    }
    unbind(endpoint) {
        this.#checkOpen();
        return this.native.unbind(endpoint);
    }
    disconnect(endpoint) {
        this.#checkOpen();
        return this.native.disconnect(endpoint);
    }
    send(message) {
        this.#checkOpen();
        return this.native.send(partsFrom(message));
    }
    sendSync(message) {
        this.#checkOpen();
        this.native.sendSync(partsFrom(message));
    }
    async recv(options = {}) {
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
    recvSync() {
        this.#checkOpen();
        return new Message(this.native.recvSync());
    }
    tryRecv() {
        this.#checkOpen();
        const parts = this.native.tryRecv();
        return parts === null ? null : new Message(parts);
    }
    waitConnectedSync(minPeers = 1, timeoutMs = 5000) {
        this.#checkOpen();
        return this.native.waitConnectedSync(minPeers, timeoutMs);
    }
    recvManySync(max, timeoutMs) {
        this.#checkOpen();
        return this.native.recvManySync(max, timeoutMs).map((parts) => new Message(parts));
    }
    close() {
        if (this.#closed) {
            return;
        }
        this.#closed = true;
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
    subscribeNative(prefix) {
        this.#checkOpen();
        return this.native.subscribe(toBytes(prefix));
    }
    unsubscribeNative(prefix) {
        this.#checkOpen();
        return this.native.unsubscribe(toBytes(prefix));
    }
    joinNative(group) {
        this.#checkOpen();
        return this.native.join(toBytes(group));
    }
    leaveNative(group) {
        this.#checkOpen();
        return this.native.leave(toBytes(group));
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
function partsFrom(input) {
    return Message.from(input).parts;
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
