//! Zig binding for OMQ through the libzmq-compatible C ABI.
//!
//! The API follows pyomq names where they make sense in Zig while keeping
//! allocator ownership explicit. Data returned from receive calls is owned by
//! the caller and must be freed with the same allocator.

const std = @import("std");

/// Raw libzmq-compatible C ABI imported from `omq-libzmq/include/zmq.h`.
pub const c = @cImport({
    @cInclude("zmq.h");
});

pub const PAIR = c.ZMQ_PAIR;
pub const PUB = c.ZMQ_PUB;
pub const SUB = c.ZMQ_SUB;
pub const REQ = c.ZMQ_REQ;
pub const REP = c.ZMQ_REP;
pub const DEALER = c.ZMQ_DEALER;
pub const ROUTER = c.ZMQ_ROUTER;
pub const PULL = c.ZMQ_PULL;
pub const PUSH = c.ZMQ_PUSH;
pub const XPUB = c.ZMQ_XPUB;
pub const XSUB = c.ZMQ_XSUB;
pub const STREAM = c.ZMQ_STREAM;
pub const SERVER = c.ZMQ_SERVER;
pub const CLIENT = c.ZMQ_CLIENT;
pub const RADIO = c.ZMQ_RADIO;
pub const DISH = c.ZMQ_DISH;
pub const GATHER = c.ZMQ_GATHER;
pub const SCATTER = c.ZMQ_SCATTER;
pub const PEER = c.ZMQ_PEER;
pub const CHANNEL = c.ZMQ_CHANNEL;

pub const DONTWAIT = c.ZMQ_DONTWAIT;
pub const NOBLOCK = c.ZMQ_NOBLOCK;
pub const SNDMORE = c.ZMQ_SNDMORE;

pub const SUBSCRIBE = c.ZMQ_SUBSCRIBE;
pub const UNSUBSCRIBE = c.ZMQ_UNSUBSCRIBE;
pub const RCVMORE = c.ZMQ_RCVMORE;
pub const TYPE = c.ZMQ_TYPE;
pub const LINGER = c.ZMQ_LINGER;
pub const SNDHWM = c.ZMQ_SNDHWM;
pub const RCVHWM = c.ZMQ_RCVHWM;
pub const RCVTIMEO = c.ZMQ_RCVTIMEO;
pub const SNDTIMEO = c.ZMQ_SNDTIMEO;
pub const SNDBUF = c.ZMQ_SNDBUF;
pub const RCVBUF = c.ZMQ_RCVBUF;
pub const FD = c.ZMQ_FD;
pub const EVENTS = c.ZMQ_EVENTS;
pub const LAST_ENDPOINT = c.ZMQ_LAST_ENDPOINT;
pub const ROUTER_MANDATORY = c.ZMQ_ROUTER_MANDATORY;
pub const IDENTITY = c.ZMQ_IDENTITY;
pub const ROUTING_ID = c.ZMQ_ROUTING_ID;
pub const HEARTBEAT_IVL = c.ZMQ_HEARTBEAT_IVL;
pub const HEARTBEAT_TTL = c.ZMQ_HEARTBEAT_TTL;
pub const HEARTBEAT_TIMEOUT = c.ZMQ_HEARTBEAT_TIMEOUT;
pub const HANDSHAKE_IVL = c.ZMQ_HANDSHAKE_IVL;
pub const MAXMSGSIZE = c.ZMQ_MAXMSGSIZE;
pub const CONFLATE = c.ZMQ_CONFLATE;
pub const TCP_KEEPALIVE = c.ZMQ_TCP_KEEPALIVE;
pub const TCP_KEEPALIVE_IDLE = c.ZMQ_TCP_KEEPALIVE_IDLE;
pub const TCP_KEEPALIVE_INTVL = c.ZMQ_TCP_KEEPALIVE_INTVL;
pub const TCP_KEEPALIVE_CNT = c.ZMQ_TCP_KEEPALIVE_CNT;
pub const CURVE_SERVER = c.ZMQ_CURVE_SERVER;
pub const CURVE_PUBLICKEY = c.ZMQ_CURVE_PUBLICKEY;
pub const CURVE_SECRETKEY = c.ZMQ_CURVE_SECRETKEY;
pub const CURVE_SERVERKEY = c.ZMQ_CURVE_SERVERKEY;
pub const PLAIN_SERVER = c.ZMQ_PLAIN_SERVER;
pub const PLAIN_USERNAME = c.ZMQ_PLAIN_USERNAME;
pub const PLAIN_PASSWORD = c.ZMQ_PLAIN_PASSWORD;
pub const AFFINITY = c.ZMQ_AFFINITY;
pub const BACKLOG = c.ZMQ_BACKLOG;
pub const IMMEDIATE = c.ZMQ_IMMEDIATE;
pub const IPV6 = c.ZMQ_IPV6;
pub const IPV4ONLY = c.ZMQ_IPV4ONLY;
pub const RATE = c.ZMQ_RATE;
pub const RECOVERY_IVL = c.ZMQ_RECOVERY_IVL;
pub const MULTICAST_HOPS = c.ZMQ_MULTICAST_HOPS;
pub const XPUB_VERBOSE = c.ZMQ_XPUB_VERBOSE;
pub const XPUB_NODROP = c.ZMQ_XPUB_NODROP;
pub const MECHANISM = c.ZMQ_MECHANISM;
pub const PROBE_ROUTER = c.ZMQ_PROBE_ROUTER;
pub const REQ_CORRELATE = c.ZMQ_REQ_CORRELATE;
pub const REQ_RELAXED = c.ZMQ_REQ_RELAXED;
pub const ZAP_DOMAIN = c.ZMQ_ZAP_DOMAIN;
pub const ROUTER_HANDOVER = c.ZMQ_ROUTER_HANDOVER;
pub const CONNECT_TIMEOUT = c.ZMQ_CONNECT_TIMEOUT;
pub const RECONNECT_IVL = c.ZMQ_RECONNECT_IVL;
pub const RECONNECT_IVL_MAX = c.ZMQ_RECONNECT_IVL_MAX;
pub const RECONNECT_STOP = c.ZMQ_RECONNECT_STOP;
pub const RECONNECT_STOP_CONN_REFUSED = c.ZMQ_RECONNECT_STOP_CONN_REFUSED;
pub const RECONNECT_STOP_HANDSHAKE_FAILED = c.ZMQ_RECONNECT_STOP_HANDSHAKE_FAILED;
pub const RECONNECT_STOP_AFTER_DISCONNECT = c.ZMQ_RECONNECT_STOP_AFTER_DISCONNECT;
pub const TCP_MAXRT = c.ZMQ_TCP_MAXRT;
pub const OMQ_ON_MUTE = c.OMQ_ON_MUTE;
pub const OMQ_COMPRESSION_LEVEL = c.OMQ_COMPRESSION_LEVEL;
pub const OMQ_COMPRESSION_DICT = c.OMQ_COMPRESSION_DICT;
pub const OMQ_COMPRESSION_AUTO_TRAIN = c.OMQ_COMPRESSION_AUTO_TRAIN;
pub const OMQ_WORKLOAD_PROFILE = c.OMQ_WORKLOAD_PROFILE;
pub const OMQ_ARENA_THRESHOLD = c.OMQ_ARENA_THRESHOLD;
pub const OMQ_ON_MUTE_BLOCK = c.OMQ_ON_MUTE_BLOCK;
pub const OMQ_ON_MUTE_DROP_NEWEST = c.OMQ_ON_MUTE_DROP_NEWEST;
pub const OMQ_ON_MUTE_DROP_OLDEST = c.OMQ_ON_MUTE_DROP_OLDEST;
pub const OMQ_WORKLOAD_DEFAULT = c.OMQ_WORKLOAD_DEFAULT;
pub const OMQ_WORKLOAD_THROUGHPUT = c.OMQ_WORKLOAD_THROUGHPUT;
pub const OMQ_WORKLOAD_LATENCY = c.OMQ_WORKLOAD_LATENCY;

pub const POLLIN = c.ZMQ_POLLIN;
pub const POLLOUT = c.ZMQ_POLLOUT;
pub const POLLERR = c.ZMQ_POLLERR;
pub const POLLPRI = c.ZMQ_POLLPRI;

pub const NULL = c.ZMQ_NULL;
pub const PLAIN = c.ZMQ_PLAIN;
pub const CURVE = c.ZMQ_CURVE;

pub const EVENT_CONNECTED = c.ZMQ_EVENT_CONNECTED;
pub const EVENT_CONNECT_DELAYED = c.ZMQ_EVENT_CONNECT_DELAYED;
pub const EVENT_CONNECT_RETRIED = c.ZMQ_EVENT_CONNECT_RETRIED;
pub const EVENT_LISTENING = c.ZMQ_EVENT_LISTENING;
pub const EVENT_BIND_FAILED = c.ZMQ_EVENT_BIND_FAILED;
pub const EVENT_ACCEPTED = c.ZMQ_EVENT_ACCEPTED;
pub const EVENT_ACCEPT_FAILED = c.ZMQ_EVENT_ACCEPT_FAILED;
pub const EVENT_CLOSED = c.ZMQ_EVENT_CLOSED;
pub const EVENT_CLOSE_FAILED = c.ZMQ_EVENT_CLOSE_FAILED;
pub const EVENT_DISCONNECTED = c.ZMQ_EVENT_DISCONNECTED;
pub const EVENT_MONITOR_STOPPED = c.ZMQ_EVENT_MONITOR_STOPPED;
pub const EVENT_HANDSHAKE_FAILED_NO_DETAIL = c.ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL;
pub const EVENT_HANDSHAKE_SUCCEEDED = c.ZMQ_EVENT_HANDSHAKE_SUCCEEDED;
pub const EVENT_HANDSHAKE_FAILED_PROTOCOL = c.ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL;
pub const EVENT_HANDSHAKE_FAILED_AUTH = c.ZMQ_EVENT_HANDSHAKE_FAILED_AUTH;
pub const EVENT_ALL = c.ZMQ_EVENT_ALL;

pub const Error = error{
    Again,
    AddressInUse,
    AddressNotAvailable,
    ContextTerminated,
    Fault,
    HostUnreachable,
    Invalid,
    Interrupted,
    MessageTooLarge,
    NoSocket,
    NotConnected,
    Protocol,
    TimedOut,
    Unsupported,
    Unroutable,
    Unknown,
};

/// Runtime ABI version returned by `zmq_version`.
pub const Version = struct {
    major: i32,
    minor: i32,
    patch: i32,
};

/// Opaque key used to create another Zig `Context` handle for the same OMQ
/// context.
pub const ShareKey = struct {
    high: u64,
    low: u64,
};

/// Z85 CURVE public/secret keypair.
pub const CurveKeypair = struct {
    public: [40]u8,
    secret: [40]u8,

    pub fn publicSlice(self: *const CurveKeypair) []const u8 {
        return self.public[0..];
    }

    pub fn secretSlice(self: *const CurveKeypair) []const u8 {
        return self.secret[0..];
    }
};

/// Owned message frame with pyomq-style metadata.
///
/// `data` and optional `group` are allocator-owned. Call `deinit` when done.
pub const Frame = struct {
    allocator: std.mem.Allocator,
    data: []u8,
    more: bool = false,
    routing_id: u32 = 0,
    group: ?[]u8 = null,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) !Frame {
        return .{
            .allocator = allocator,
            .data = try allocator.dupe(u8, data),
        };
    }

    pub fn deinit(self: *Frame) void {
        self.allocator.free(self.data);
        if (self.group) |group| self.allocator.free(group);
        self.data = &.{};
        self.group = null;
    }
};

/// Owned multipart message.
///
/// Every part is allocator-owned. Call `deinit` when done.
pub const Message = struct {
    allocator: std.mem.Allocator,
    parts: [][]u8,

    pub fn deinit(self: *Message) void {
        for (self.parts) |part| {
            self.allocator.free(part);
        }
        self.allocator.free(self.parts);
        self.parts = &.{};
    }

    pub fn single(self: *const Message) ?[]const u8 {
        if (self.parts.len != 1) return null;
        return self.parts[0];
    }
};

/// Owned multipart message preserving per-frame metadata.
///
/// Every frame is allocator-owned. Call `deinit` when done.
pub const FrameMessage = struct {
    allocator: std.mem.Allocator,
    frames: []Frame,

    pub fn deinit(self: *FrameMessage) void {
        for (self.frames) |*frame| {
            frame.deinit();
        }
        self.allocator.free(self.frames);
        self.frames = &.{};
    }
};

pub const PollItem = c.zmq_pollitem_t;

pub const PollEvent = struct {
    socket: *Socket,
    events: i16,
};

pub const BindRandomPortOptions = struct {
    min_port: u16 = 49152,
    max_port: u16 = 65535,
    max_tries: usize = 100,
};

const PollRegistration = struct {
    socket: *Socket,
    events: i16,
};

/// Small pyomq-like poller around `zmq_poll`.
pub const Poller = struct {
    allocator: std.mem.Allocator,
    registrations: std.array_list.Managed(PollRegistration),
    items: std.array_list.Managed(PollItem),

    pub fn init(allocator: std.mem.Allocator) Poller {
        return .{
            .allocator = allocator,
            .registrations = .init(allocator),
            .items = .init(allocator),
        };
    }

    pub fn deinit(self: *Poller) void {
        self.items.deinit();
        self.registrations.deinit();
    }

    pub fn register(self: *Poller, socket: *Socket, events: i16) !void {
        for (self.registrations.items) |*entry| {
            if (entry.socket == socket) {
                entry.events = events;
                return;
            }
        }
        try self.registrations.append(.{ .socket = socket, .events = events });
    }

    pub fn modify(self: *Poller, socket: *Socket, events: i16) Error!void {
        for (self.registrations.items) |*entry| {
            if (entry.socket == socket) {
                entry.events = events;
                return;
            }
        }
        return Error.NoSocket;
    }

    pub fn unregister(self: *Poller, socket: *Socket) Error!void {
        for (self.registrations.items, 0..) |entry, index| {
            if (entry.socket == socket) {
                _ = self.registrations.orderedRemove(index);
                return;
            }
        }
        return Error.NoSocket;
    }

    pub fn pollAlloc(self: *Poller, timeout_ms: i64) ![]PollEvent {
        self.items.clearRetainingCapacity();
        try self.items.ensureTotalCapacity(self.registrations.items.len);
        for (self.registrations.items) |entry| {
            self.items.appendAssumeCapacity(try entry.socket.pollItem(entry.events));
        }

        _ = try poll(self.items.items, timeout_ms);

        var events: std.array_list.Managed(PollEvent) = .init(self.allocator);
        errdefer events.deinit();
        for (self.registrations.items, self.items.items) |registration, item| {
            if (item.revents != 0) {
                try events.append(.{
                    .socket = registration.socket,
                    .events = item.revents,
                });
            }
        }
        return events.toOwnedSlice();
    }
};

/// Parsed socket monitor event.
///
/// `endpoint` is allocator-owned. Call `deinit` when done.
pub const MonitorEvent = struct {
    allocator: std.mem.Allocator,
    event: u16,
    value: u32,
    endpoint: []u8,

    pub fn deinit(self: *MonitorEvent) void {
        self.allocator.free(self.endpoint);
        self.endpoint = &.{};
    }
};

/// PAIR socket wrapper connected to a monitored socket endpoint.
pub const Monitor = struct {
    socket: Socket,

    pub fn deinit(self: *Monitor) void {
        self.socket.deinit();
    }

    pub fn close(self: *Monitor) Error!void {
        try self.socket.close();
    }

    pub fn recvAlloc(self: *Monitor, allocator: std.mem.Allocator, flags: i32) !MonitorEvent {
        const header = try self.socket.recvAlloc(allocator, flags);
        defer allocator.free(header);
        if (header.len < 6) return Error.Protocol;

        return .{
            .allocator = allocator,
            .event = std.mem.readInt(u16, header[0..2], .little),
            .value = std.mem.readInt(u32, header[2..6], .little),
            .endpoint = try self.socket.recvAlloc(allocator, flags),
        };
    }

    pub fn setReceiveTimeout(self: *Monitor, millis: i32) Error!void {
        try self.socket.setReceiveTimeout(millis);
    }
};

/// OMQ context. Owns the underlying libzmq-compatible context handle.
pub const Context = struct {
    raw: ?*anyopaque,

    pub fn init() Error!Context {
        return initWithIoThreads(1);
    }

    pub fn initWithIoThreads(io_threads: i32) Error!Context {
        const ctx = c.zmq_ctx_new() orelse return mapErrno();
        errdefer _ = c.zmq_ctx_term(ctx);

        try setCtxInt(ctx, c.ZMQ_IO_THREADS, @max(io_threads, 1));
        return .{ .raw = ctx };
    }

    pub fn deinit(self: *Context) void {
        if (self.raw) |ctx| {
            self.raw = null;
            _ = c.zmq_ctx_term(ctx);
        }
    }

    pub fn closed(self: *const Context) bool {
        return self.raw == null;
    }

    pub fn term(self: *Context) Error!void {
        const ctx = self.raw orelse return;
        self.raw = null;
        try check(c.zmq_ctx_term(ctx));
    }

    pub fn shutdown(self: *Context) Error!void {
        try check(c.zmq_ctx_shutdown(try self.ptr()));
    }

    pub fn socket(self: *Context, socket_type: i32) Error!Socket {
        const raw = c.zmq_socket(try self.ptr(), socket_type) orelse return mapErrno();
        return .{ .raw = raw };
    }

    pub fn shareKey(self: *Context) Error!ShareKey {
        var high: u64 = 0;
        var low: u64 = 0;
        try check(c.omq_ctx_share_key(try self.ptr(), &high, &low));
        return .{ .high = high, .low = low };
    }

    pub fn fromShareKey(key: ShareKey) Error!Context {
        const ctx = c.omq_ctx_from_share_key(key.high, key.low) orelse return mapErrno();
        return .{ .raw = ctx };
    }

    fn setInt(self: *Context, option: i32, value: i32) Error!void {
        try setCtxInt(try self.ptr(), option, value);
    }

    fn getInt(self: *Context, option: i32) Error!i32 {
        const rc = c.zmq_ctx_get(try self.ptr(), option);
        if (rc == -1) return mapErrno();
        return rc;
    }

    fn ptr(self: *Context) Error!*anyopaque {
        return self.raw orelse Error.ContextTerminated;
    }
};

/// OMQ socket. One socket should be used from one thread unless migration is
/// explicitly enabled by `allowThreadMigration`.
pub const Socket = struct {
    raw: ?*anyopaque,

    pub fn deinit(self: *Socket) void {
        if (self.raw) |socket_raw| {
            self.raw = null;
            _ = c.zmq_close(socket_raw);
        }
    }

    pub fn closed(self: *const Socket) bool {
        return self.raw == null;
    }

    pub fn close(self: *Socket) Error!void {
        const socket_raw = self.raw orelse return;
        self.raw = null;
        try check(c.zmq_close(socket_raw));
    }

    pub fn bind(self: *Socket, allocator: std.mem.Allocator, endpoint: []const u8) ![]u8 {
        const endpoint_z = try allocator.dupeZ(u8, endpoint);
        defer allocator.free(endpoint_z);
        try check(c.zmq_bind(try self.ptr(), endpoint_z.ptr));
        return try self.getLastEndpoint(allocator);
    }

    /// Bind to a TCP port and return the selected port. Default options ask
    /// the OS for a free port with `:0`. Custom ranges try concrete ports in
    /// order, bounded by `max_tries`.
    pub fn bindToRandomPort(
        self: *Socket,
        allocator: std.mem.Allocator,
        addr: []const u8,
        options: BindRandomPortOptions,
    ) !u16 {
        if (options.min_port > options.max_port or options.max_tries == 0) return Error.Invalid;

        if (std.meta.eql(options, BindRandomPortOptions{})) {
            const endpoint = try std.fmt.allocPrint(allocator, "{s}:0", .{addr});
            defer allocator.free(endpoint);

            const bound = try self.bind(allocator, endpoint);
            defer allocator.free(bound);
            return try endpointPort(bound);
        }

        const range_len = @as(usize, options.max_port - options.min_port) + 1;
        const attempts = @min(options.max_tries, range_len);
        for (0..attempts) |offset| {
            const port = options.min_port + @as(u16, @intCast(offset));
            const endpoint = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ addr, port });
            defer allocator.free(endpoint);

            const bound = self.bind(allocator, endpoint) catch |err| switch (err) {
                Error.AddressInUse => continue,
                else => return err,
            };
            allocator.free(bound);
            return port;
        }
        return Error.AddressInUse;
    }

    pub fn connect(self: *Socket, allocator: std.mem.Allocator, endpoint: []const u8) !void {
        const endpoint_z = try allocator.dupeZ(u8, endpoint);
        defer allocator.free(endpoint_z);
        try check(c.zmq_connect(try self.ptr(), endpoint_z.ptr));
    }

    pub fn unbind(self: *Socket, allocator: std.mem.Allocator, endpoint: []const u8) !void {
        const endpoint_z = try allocator.dupeZ(u8, endpoint);
        defer allocator.free(endpoint_z);
        try check(c.zmq_unbind(try self.ptr(), endpoint_z.ptr));
    }

    pub fn disconnect(self: *Socket, allocator: std.mem.Allocator, endpoint: []const u8) !void {
        const endpoint_z = try allocator.dupeZ(u8, endpoint);
        defer allocator.free(endpoint_z);
        try check(c.zmq_disconnect(try self.ptr(), endpoint_z.ptr));
    }

    pub fn send(self: *Socket, data: []const u8, flags: i32) Error!usize {
        const sent = c.zmq_send(try self.ptr(), dataPtr(data), data.len, flags);
        if (sent == -1) return mapErrno();
        return @intCast(sent);
    }

    pub fn sendString(self: *Socket, data: []const u8) Error!usize {
        return self.send(data, 0);
    }

    pub fn recvInto(self: *Socket, buffer: []u8, flags: i32) Error!usize {
        const received = c.zmq_recv(try self.ptr(), buffer.ptr, buffer.len, flags);
        if (received == -1) return mapErrno();
        return @intCast(received);
    }

    pub fn sendMultipart(self: *Socket, parts: []const []const u8, flags: i32) Error!void {
        for (parts, 0..) |part, index| {
            const more = if (index + 1 == parts.len) 0 else SNDMORE;
            _ = try self.send(part, flags | more);
        }
    }

    pub fn sendGroup(
        self: *Socket,
        allocator: std.mem.Allocator,
        group: []const u8,
        data: []const u8,
        flags: i32,
    ) !void {
        var msg: c.zmq_msg_t = undefined;
        try check(c.zmq_msg_init_size(&msg, data.len));
        defer _ = c.zmq_msg_close(&msg);

        const dst: [*]u8 = @ptrCast(c.zmq_msg_data(&msg).?);
        @memcpy(dst[0..data.len], data);

        const group_z = try allocator.dupeZ(u8, group);
        defer allocator.free(group_z);
        try check(c.zmq_msg_set_group(&msg, group_z.ptr));
        if (c.zmq_msg_send(&msg, try self.ptr(), flags) == -1) return mapErrno();
    }

    pub fn sendFrame(self: *Socket, allocator: std.mem.Allocator, frame: *const Frame, flags: i32) !void {
        var msg: c.zmq_msg_t = undefined;
        try check(c.zmq_msg_init_size(&msg, frame.data.len));
        defer _ = c.zmq_msg_close(&msg);

        const dst: [*]u8 = @ptrCast(c.zmq_msg_data(&msg).?);
        @memcpy(dst[0..frame.data.len], frame.data);
        if (frame.routing_id != 0) {
            try check(c.zmq_msg_set_routing_id(&msg, frame.routing_id));
        }
        if (frame.group) |group| {
            const group_z = try allocator.dupeZ(u8, group);
            defer allocator.free(group_z);
            try check(c.zmq_msg_set_group(&msg, group_z.ptr));
        }
        if (c.zmq_msg_send(&msg, try self.ptr(), flags) == -1) return mapErrno();
    }

    pub fn recvAlloc(self: *Socket, allocator: std.mem.Allocator, flags: i32) ![]u8 {
        var msg: c.zmq_msg_t = undefined;
        try check(c.zmq_msg_init(&msg));
        defer _ = c.zmq_msg_close(&msg);

        if (c.zmq_msg_recv(&msg, try self.ptr(), flags) == -1) return mapErrno();

        const len = c.zmq_msg_size(&msg);
        return allocator.dupe(u8, try msgDataSlice(&msg, len));
    }

    pub fn recvFrameAlloc(self: *Socket, allocator: std.mem.Allocator, flags: i32) !Frame {
        var msg: c.zmq_msg_t = undefined;
        try check(c.zmq_msg_init(&msg));
        defer _ = c.zmq_msg_close(&msg);

        if (c.zmq_msg_recv(&msg, try self.ptr(), flags) == -1) return mapErrno();

        const len = c.zmq_msg_size(&msg);
        const data = try allocator.dupe(u8, try msgDataSlice(&msg, len));
        errdefer allocator.free(data);

        const group = c.zmq_msg_group(&msg);
        const group_copy = if (group == null) null else try allocator.dupe(u8, std.mem.span(group));
        errdefer if (group_copy) |owned| allocator.free(owned);

        return .{
            .allocator = allocator,
            .data = data,
            .more = c.zmq_msg_more(&msg) != 0,
            .routing_id = c.zmq_msg_routing_id(&msg),
            .group = group_copy,
        };
    }

    pub fn recvMultipartAlloc(
        self: *Socket,
        allocator: std.mem.Allocator,
        flags: i32,
    ) !Message {
        var parts: std.array_list.Managed([]u8) = .init(allocator);
        errdefer {
            for (parts.items) |part| allocator.free(part);
            parts.deinit();
        }

        while (true) {
            const part = try self.recvAlloc(allocator, flags);
            errdefer allocator.free(part);
            try parts.append(part);
            if (try self.getInt(RCVMORE) == 0) break;
        }

        return .{
            .allocator = allocator,
            .parts = try parts.toOwnedSlice(),
        };
    }

    pub fn recvMultipartFramesAlloc(
        self: *Socket,
        allocator: std.mem.Allocator,
        flags: i32,
    ) !FrameMessage {
        var frames: std.array_list.Managed(Frame) = .init(allocator);
        errdefer {
            for (frames.items) |*frame| frame.deinit();
            frames.deinit();
        }

        while (true) {
            const frame = try self.recvFrameAlloc(allocator, flags);
            const more = frame.more;
            errdefer {
                var mutable = frame;
                mutable.deinit();
            }
            try frames.append(frame);
            if (!more) break;
        }

        return .{
            .allocator = allocator,
            .frames = try frames.toOwnedSlice(),
        };
    }

    pub fn subscribe(self: *Socket, prefix: []const u8) Error!void {
        try self.setBytes(SUBSCRIBE, prefix);
    }

    pub fn unsubscribe(self: *Socket, prefix: []const u8) Error!void {
        try self.setBytes(UNSUBSCRIBE, prefix);
    }

    pub fn join(self: *Socket, allocator: std.mem.Allocator, group: []const u8) !void {
        const group_z = try allocator.dupeZ(u8, group);
        defer allocator.free(group_z);
        try check(c.zmq_join(try self.ptr(), group_z.ptr));
    }

    pub fn leave(self: *Socket, allocator: std.mem.Allocator, group: []const u8) !void {
        const group_z = try allocator.dupeZ(u8, group);
        defer allocator.free(group_z);
        try check(c.zmq_leave(try self.ptr(), group_z.ptr));
    }

    fn setInt(self: *Socket, option: i32, value: i32) Error!void {
        var raw_value: c_int = @intCast(value);
        try check(c.zmq_setsockopt(
            try self.ptr(),
            option,
            &raw_value,
            @sizeOf(c_int),
        ));
    }

    fn setI64(self: *Socket, option: i32, value: i64) Error!void {
        var raw_value: i64 = value;
        try check(c.zmq_setsockopt(
            try self.ptr(),
            option,
            &raw_value,
            @sizeOf(i64),
        ));
    }

    fn setBytes(self: *Socket, option: i32, data: []const u8) Error!void {
        try check(c.zmq_setsockopt(try self.ptr(), option, dataPtr(data), data.len));
    }

    fn setString(self: *Socket, allocator: std.mem.Allocator, option: i32, value: []const u8) !void {
        const value_z = try allocator.dupeZ(u8, value);
        defer allocator.free(value_z);
        try check(c.zmq_setsockopt(try self.ptr(), option, value_z.ptr, value.len));
    }

    fn getInt(self: *Socket, option: i32) Error!i32 {
        var value: c_int = 0;
        var len: usize = @sizeOf(c_int);
        try check(c.zmq_getsockopt(try self.ptr(), option, &value, &len));
        return value;
    }

    fn getI64(self: *Socket, option: i32) Error!i64 {
        var value: i64 = 0;
        var len: usize = @sizeOf(i64);
        try check(c.zmq_getsockopt(try self.ptr(), option, &value, &len));
        return value;
    }

    fn getBytesAlloc(self: *Socket, allocator: std.mem.Allocator, option: i32, capacity: usize) ![]u8 {
        const buffer = try allocator.alloc(u8, capacity);
        errdefer allocator.free(buffer);

        var len = capacity;
        try check(c.zmq_getsockopt(try self.ptr(), option, buffer.ptr, &len));
        return try allocator.realloc(buffer, len);
    }

    fn getStringAlloc(self: *Socket, allocator: std.mem.Allocator, option: i32, capacity: usize) ![]u8 {
        const bytes = try self.getBytesAlloc(allocator, option, capacity);
        errdefer allocator.free(bytes);
        const end = std.mem.indexOfScalar(u8, bytes, 0) orelse bytes.len;
        return try allocator.realloc(bytes, end);
    }

    pub fn getLastEndpoint(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        var buffer: [512]u8 = undefined;
        var len: usize = buffer.len;
        try check(c.zmq_getsockopt(try self.ptr(), LAST_ENDPOINT, &buffer, &len));
        const end = std.mem.indexOfScalar(u8, buffer[0..len], 0) orelse len;
        return allocator.dupe(u8, buffer[0..end]);
    }

    pub fn pollItem(self: *Socket, event_mask: i16) Error!PollItem {
        return .{
            .socket = try self.ptr(),
            .fd = 0,
            .events = event_mask,
            .revents = 0,
        };
    }

    /// Poll this socket and return the event mask, or 0 on timeout.
    pub fn poll(self: *Socket, timeout_ms: i64, event_mask: i16) Error!i16 {
        var item = try self.pollItem(event_mask);
        const rc = c.zmq_poll(&item, 1, @intCast(timeout_ms));
        if (rc == -1) return mapErrno();
        return item.revents;
    }

    pub fn socketType(self: *Socket) Error!i32 {
        return self.getInt(TYPE);
    }

    pub fn hasReceiveMore(self: *Socket) Error!bool {
        return try self.getInt(RCVMORE) != 0;
    }

    pub fn fd(self: *Socket) Error!i32 {
        return self.getInt(FD);
    }

    pub fn pollEvents(self: *Socket) Error!i32 {
        return self.getInt(EVENTS);
    }

    pub fn setLinger(self: *Socket, millis: i32) Error!void {
        try self.setInt(LINGER, millis);
    }

    pub fn linger(self: *Socket) Error!i32 {
        return self.getInt(LINGER);
    }

    pub fn setSendTimeout(self: *Socket, millis: i32) Error!void {
        try self.setInt(SNDTIMEO, millis);
    }

    pub fn sendTimeout(self: *Socket) Error!i32 {
        return self.getInt(SNDTIMEO);
    }

    pub fn setReceiveTimeout(self: *Socket, millis: i32) Error!void {
        try self.setInt(RCVTIMEO, millis);
    }

    pub fn receiveTimeout(self: *Socket) Error!i32 {
        return self.getInt(RCVTIMEO);
    }

    pub fn setSendHighWaterMark(self: *Socket, value: i32) Error!void {
        try self.setInt(SNDHWM, value);
    }

    pub fn sendHighWaterMark(self: *Socket) Error!i32 {
        return self.getInt(SNDHWM);
    }

    pub fn setReceiveHighWaterMark(self: *Socket, value: i32) Error!void {
        try self.setInt(RCVHWM, value);
    }

    pub fn receiveHighWaterMark(self: *Socket) Error!i32 {
        return self.getInt(RCVHWM);
    }

    pub fn setHighWaterMark(self: *Socket, value: i32) Error!void {
        try self.setSendHighWaterMark(value);
        try self.setReceiveHighWaterMark(value);
    }

    pub fn highWaterMark(self: *Socket) Error!i32 {
        return self.getInt(SNDHWM);
    }

    pub fn setIdentity(self: *Socket, value: []const u8) Error!void {
        try self.setBytes(IDENTITY, value);
    }

    pub fn identityAlloc(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        return self.getBytesAlloc(allocator, IDENTITY, 255);
    }

    pub fn setSendBufferSize(self: *Socket, bytes: i32) Error!void {
        try self.setInt(SNDBUF, bytes);
    }

    pub fn sendBufferSize(self: *Socket) Error!i32 {
        return self.getInt(SNDBUF);
    }

    pub fn setReceiveBufferSize(self: *Socket, bytes: i32) Error!void {
        try self.setInt(RCVBUF, bytes);
    }

    pub fn receiveBufferSize(self: *Socket) Error!i32 {
        return self.getInt(RCVBUF);
    }

    pub fn setRouterMandatory(self: *Socket, enabled: bool) Error!void {
        try self.setInt(ROUTER_MANDATORY, @intFromBool(enabled));
    }

    pub fn routerMandatory(self: *Socket) Error!bool {
        return try self.getInt(ROUTER_MANDATORY) != 0;
    }

    pub fn setConflate(self: *Socket, enabled: bool) Error!void {
        try self.setInt(CONFLATE, @intFromBool(enabled));
    }

    pub fn conflate(self: *Socket) Error!bool {
        return try self.getInt(CONFLATE) != 0;
    }

    pub fn setImmediate(self: *Socket, enabled: bool) Error!void {
        try self.setInt(IMMEDIATE, @intFromBool(enabled));
    }

    pub fn immediate(self: *Socket) Error!bool {
        return try self.getInt(IMMEDIATE) != 0;
    }

    pub fn setIpv6(self: *Socket, enabled: bool) Error!void {
        try self.setInt(IPV6, @intFromBool(enabled));
    }

    pub fn ipv6(self: *Socket) Error!bool {
        return try self.getInt(IPV6) != 0;
    }

    pub fn setIpv4Only(self: *Socket, enabled: bool) Error!void {
        try self.setInt(IPV4ONLY, @intFromBool(enabled));
    }

    pub fn ipv4Only(self: *Socket) Error!bool {
        return try self.getInt(IPV4ONLY) != 0;
    }

    pub fn setTcpKeepalive(self: *Socket, enabled: bool) Error!void {
        try self.setInt(TCP_KEEPALIVE, @intFromBool(enabled));
    }

    pub fn tcpKeepalive(self: *Socket) Error!bool {
        return try self.getInt(TCP_KEEPALIVE) != 0;
    }

    pub fn setTcpKeepaliveIdle(self: *Socket, seconds: i32) Error!void {
        try self.setInt(TCP_KEEPALIVE_IDLE, seconds);
    }

    pub fn tcpKeepaliveIdle(self: *Socket) Error!i32 {
        return self.getInt(TCP_KEEPALIVE_IDLE);
    }

    pub fn setTcpKeepaliveInterval(self: *Socket, seconds: i32) Error!void {
        try self.setInt(TCP_KEEPALIVE_INTVL, seconds);
    }

    pub fn tcpKeepaliveInterval(self: *Socket) Error!i32 {
        return self.getInt(TCP_KEEPALIVE_INTVL);
    }

    pub fn setTcpKeepaliveCount(self: *Socket, count: i32) Error!void {
        try self.setInt(TCP_KEEPALIVE_CNT, count);
    }

    pub fn tcpKeepaliveCount(self: *Socket) Error!i32 {
        return self.getInt(TCP_KEEPALIVE_CNT);
    }

    pub fn setTcpMaxRetransmitTimeout(self: *Socket, millis: i32) Error!void {
        try self.setInt(TCP_MAXRT, millis);
    }

    pub fn setXpubNoDrop(self: *Socket, enabled: bool) Error!void {
        try self.setInt(XPUB_NODROP, @intFromBool(enabled));
    }

    pub fn xpubNoDrop(self: *Socket) Error!bool {
        return try self.getInt(XPUB_NODROP) != 0;
    }

    pub fn setXpubVerbose(self: *Socket, enabled: bool) Error!void {
        try self.setInt(XPUB_VERBOSE, @intFromBool(enabled));
    }

    pub fn xpubVerbose(self: *Socket) Error!bool {
        return try self.getInt(XPUB_VERBOSE) != 0;
    }

    pub fn setProbeRouter(self: *Socket, enabled: bool) Error!void {
        try self.setInt(PROBE_ROUTER, @intFromBool(enabled));
    }

    pub fn probeRouter(self: *Socket) Error!bool {
        return try self.getInt(PROBE_ROUTER) != 0;
    }

    pub fn setReqCorrelate(self: *Socket, enabled: bool) Error!void {
        try self.setInt(REQ_CORRELATE, @intFromBool(enabled));
    }

    pub fn reqCorrelate(self: *Socket) Error!bool {
        return try self.getInt(REQ_CORRELATE) != 0;
    }

    pub fn setReqRelaxed(self: *Socket, enabled: bool) Error!void {
        try self.setInt(REQ_RELAXED, @intFromBool(enabled));
    }

    pub fn reqRelaxed(self: *Socket) Error!bool {
        return try self.getInt(REQ_RELAXED) != 0;
    }

    pub fn setRouterHandover(self: *Socket, enabled: bool) Error!void {
        try self.setInt(ROUTER_HANDOVER, @intFromBool(enabled));
    }

    pub fn routerHandover(self: *Socket) Error!bool {
        return try self.getInt(ROUTER_HANDOVER) != 0;
    }

    pub fn setPlainServer(self: *Socket, enabled: bool) Error!void {
        try self.setInt(PLAIN_SERVER, @intFromBool(enabled));
    }

    pub fn plainServer(self: *Socket) Error!bool {
        return try self.getInt(PLAIN_SERVER) != 0;
    }

    pub fn setPlainClient(
        self: *Socket,
        allocator: std.mem.Allocator,
        username: []const u8,
        password: []const u8,
    ) !void {
        try self.setString(allocator, PLAIN_USERNAME, username);
        try self.setString(allocator, PLAIN_PASSWORD, password);
    }

    pub fn plainUsernameAlloc(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        return self.getStringAlloc(allocator, PLAIN_USERNAME, 64);
    }

    pub fn plainPasswordAlloc(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        return self.getStringAlloc(allocator, PLAIN_PASSWORD, 64);
    }

    pub fn mechanism(self: *Socket) Error!i32 {
        return self.getInt(MECHANISM);
    }

    pub fn curveServer(self: *Socket) Error!bool {
        return try self.getInt(CURVE_SERVER) != 0;
    }

    pub fn setCurvePublicKey(self: *Socket, allocator: std.mem.Allocator, public_key: []const u8) !void {
        try self.setString(allocator, CURVE_PUBLICKEY, public_key);
    }

    pub fn curvePublicKeyAlloc(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        return self.getStringAlloc(allocator, CURVE_PUBLICKEY, 41);
    }

    pub fn setCurveSecretKey(self: *Socket, allocator: std.mem.Allocator, secret_key: []const u8) !void {
        try self.setString(allocator, CURVE_SECRETKEY, secret_key);
    }

    pub fn curveSecretKeyAlloc(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        return self.getStringAlloc(allocator, CURVE_SECRETKEY, 41);
    }

    pub fn setCurveServerKey(self: *Socket, allocator: std.mem.Allocator, server_key: []const u8) !void {
        try self.setString(allocator, CURVE_SERVERKEY, server_key);
    }

    pub fn curveServerKeyAlloc(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        return self.getStringAlloc(allocator, CURVE_SERVERKEY, 41);
    }

    pub fn setCurveServer(
        self: *Socket,
        allocator: std.mem.Allocator,
        _public_key: []const u8,
        secret_key: []const u8,
    ) !void {
        _ = _public_key;
        try self.setInt(CURVE_SERVER, 1);
        try self.setCurveSecretKey(allocator, secret_key);
    }

    pub fn setCurveClient(
        self: *Socket,
        allocator: std.mem.Allocator,
        public_key: []const u8,
        secret_key: []const u8,
        server_key: []const u8,
    ) !void {
        try self.setCurvePublicKey(allocator, public_key);
        try self.setCurveSecretKey(allocator, secret_key);
        try self.setCurveServerKey(allocator, server_key);
    }

    pub fn setOnMute(self: *Socket, mode: i32) Error!void {
        try self.setInt(OMQ_ON_MUTE, mode);
    }

    pub fn onMute(self: *Socket) Error!i32 {
        return self.getInt(OMQ_ON_MUTE);
    }

    pub fn setCompressionLevel(self: *Socket, level: i32) Error!void {
        try self.setInt(OMQ_COMPRESSION_LEVEL, level);
    }

    pub fn compressionLevel(self: *Socket) Error!i32 {
        return self.getInt(OMQ_COMPRESSION_LEVEL);
    }

    pub fn setCompressionDict(self: *Socket, dict: []const u8) Error!void {
        try self.setBytes(OMQ_COMPRESSION_DICT, dict);
    }

    pub fn compressionDictAlloc(self: *Socket, allocator: std.mem.Allocator) ![]u8 {
        return self.getBytesAlloc(allocator, OMQ_COMPRESSION_DICT, 8 * 1024);
    }

    pub fn setCompressionAutoTrain(self: *Socket, enabled: bool) Error!void {
        try self.setInt(OMQ_COMPRESSION_AUTO_TRAIN, @intFromBool(enabled));
    }

    pub fn compressionAutoTrain(self: *Socket) Error!bool {
        return try self.getInt(OMQ_COMPRESSION_AUTO_TRAIN) != 0;
    }

    pub fn setWorkloadProfile(self: *Socket, profile: i32) Error!void {
        try self.setInt(OMQ_WORKLOAD_PROFILE, profile);
    }

    pub fn workloadProfile(self: *Socket) Error!i32 {
        return self.getInt(OMQ_WORKLOAD_PROFILE);
    }

    pub fn setReconnectInterval(self: *Socket, millis: i32) Error!void {
        try self.setInt(RECONNECT_IVL, millis);
    }

    pub fn reconnectInterval(self: *Socket) Error!i32 {
        return self.getInt(RECONNECT_IVL);
    }

    pub fn setMaxReconnectInterval(self: *Socket, millis: i32) Error!void {
        try self.setInt(RECONNECT_IVL_MAX, millis);
    }

    pub fn maxReconnectInterval(self: *Socket) Error!i32 {
        return self.getInt(RECONNECT_IVL_MAX);
    }

    pub fn setReconnectStop(self: *Socket, flags: i32) Error!void {
        try self.setInt(RECONNECT_STOP, flags);
    }

    pub fn reconnectStop(self: *Socket) Error!i32 {
        return self.getInt(RECONNECT_STOP);
    }

    pub fn setHeartbeatInterval(self: *Socket, millis: i32) Error!void {
        try self.setInt(HEARTBEAT_IVL, millis);
    }

    pub fn heartbeatInterval(self: *Socket) Error!i32 {
        return self.getInt(HEARTBEAT_IVL);
    }

    pub fn setHeartbeatTtl(self: *Socket, millis: i32) Error!void {
        try self.setInt(HEARTBEAT_TTL, millis);
    }

    pub fn heartbeatTtl(self: *Socket) Error!i32 {
        return self.getInt(HEARTBEAT_TTL);
    }

    pub fn setHeartbeatTimeout(self: *Socket, millis: i32) Error!void {
        try self.setInt(HEARTBEAT_TIMEOUT, millis);
    }

    pub fn heartbeatTimeout(self: *Socket) Error!i32 {
        return self.getInt(HEARTBEAT_TIMEOUT);
    }

    pub fn setHandshakeInterval(self: *Socket, millis: i32) Error!void {
        try self.setInt(HANDSHAKE_IVL, millis);
    }

    pub fn handshakeInterval(self: *Socket) Error!i32 {
        return self.getInt(HANDSHAKE_IVL);
    }

    pub fn setConnectTimeout(self: *Socket, millis: i32) Error!void {
        try self.setInt(CONNECT_TIMEOUT, millis);
    }

    pub fn connectTimeout(self: *Socket) Error!i32 {
        return self.getInt(CONNECT_TIMEOUT);
    }

    pub fn setMaxMessageSize(self: *Socket, bytes: i64) Error!void {
        try self.setI64(MAXMSGSIZE, bytes);
    }

    pub fn maxMessageSize(self: *Socket) Error!i64 {
        return self.getI64(MAXMSGSIZE);
    }

    pub fn setRate(self: *Socket, rate: i32) Error!void {
        try self.setInt(RATE, rate);
    }

    pub fn setRecoveryInterval(self: *Socket, millis: i32) Error!void {
        try self.setInt(RECOVERY_IVL, millis);
    }

    pub fn setMulticastHops(self: *Socket, hops: i32) Error!void {
        try self.setInt(MULTICAST_HOPS, hops);
    }

    pub fn setZapDomain(self: *Socket, allocator: std.mem.Allocator, domain: []const u8) !void {
        try self.setString(allocator, ZAP_DOMAIN, domain);
    }

    pub fn setArenaThreshold(self: *Socket, bytes: i64) Error!void {
        try self.setI64(OMQ_ARENA_THRESHOLD, bytes);
    }

    pub fn arenaThreshold(self: *Socket) Error!i64 {
        return self.getI64(OMQ_ARENA_THRESHOLD);
    }

    pub fn allowThreadMigration(self: *Socket) Error!void {
        try check(c.omq_socket_allow_thread_migration(try self.ptr()));
    }

    pub fn monitor(
        self: *Socket,
        ctx: *Context,
        allocator: std.mem.Allocator,
        endpoint: []const u8,
        event_mask: i32,
    ) !Monitor {
        const endpoint_z = try allocator.dupeZ(u8, endpoint);
        defer allocator.free(endpoint_z);
        try check(c.zmq_socket_monitor(try self.ptr(), endpoint_z.ptr, event_mask));

        var monitor_socket = try ctx.socket(PAIR);
        errdefer monitor_socket.deinit();
        try monitor_socket.connect(allocator, endpoint);
        return .{ .socket = monitor_socket };
    }

    fn ptr(self: *Socket) Error!*anyopaque {
        return self.raw orelse Error.NoSocket;
    }
};

/// Return the libzmq-compatible ABI version exposed by OMQ.
pub fn version() Version {
    var major: c_int = 0;
    var minor: c_int = 0;
    var patch: c_int = 0;
    c.zmq_version(&major, &minor, &patch);
    return .{
        .major = major,
        .minor = minor,
        .patch = patch,
    };
}

/// Probe a runtime capability such as `"curve"`, `"ipc"`, or `"draft"`.
pub fn has(allocator: std.mem.Allocator, capability: []const u8) !bool {
    const capability_z = try allocator.dupeZ(u8, capability);
    defer allocator.free(capability_z);
    return c.zmq_has(capability_z.ptr) == 1;
}

/// Generate a Z85 CURVE keypair.
pub fn curveKeypair() Error!CurveKeypair {
    var public_z: [41]u8 = undefined;
    var secret_z: [41]u8 = undefined;
    try check(c.zmq_curve_keypair(&public_z, &secret_z));
    return .{
        .public = public_z[0..40].*,
        .secret = secret_z[0..40].*,
    };
}

/// Derive the Z85 public CURVE key for a Z85 secret key.
pub fn curvePublic(allocator: std.mem.Allocator, secret: []const u8) ![40]u8 {
    const secret_z = try allocator.dupeZ(u8, secret);
    defer allocator.free(secret_z);

    var public_z: [41]u8 = undefined;
    try check(c.zmq_curve_public(&public_z, secret_z.ptr));
    return public_z[0..40].*;
}

/// Poll raw `PollItem` values and return the number with events.
pub fn poll(items: []PollItem, timeout_ms: i64) Error!usize {
    const rc = c.zmq_poll(items.ptr, @intCast(items.len), @intCast(timeout_ms));
    if (rc == -1) return mapErrno();
    return @intCast(rc);
}

/// Run the built-in proxy device until interrupted or terminated.
pub fn proxy(frontend: *Socket, backend: *Socket, capture: ?*Socket) Error!void {
    const capture_raw = if (capture) |socket| try socket.ptr() else null;
    try check(c.zmq_proxy(try frontend.ptr(), try backend.ptr(), capture_raw));
}

/// Run the built-in steerable proxy device.
pub fn proxySteerable(
    frontend: *Socket,
    backend: *Socket,
    capture: ?*Socket,
    control: ?*Socket,
) Error!void {
    const capture_raw = if (capture) |socket| try socket.ptr() else null;
    const control_raw = if (control) |socket| try socket.ptr() else null;
    try check(c.zmq_proxy_steerable(try frontend.ptr(), try backend.ptr(), capture_raw, control_raw));
}

/// Return the last ABI errno for the current thread.
pub fn lastErrno() i32 {
    return c.zmq_errno();
}

/// Return ABI error text for an errno value.
pub fn strerror(errnum: i32) []const u8 {
    return std.mem.span(c.zmq_strerror(errnum));
}

fn setCtxInt(ctx: *anyopaque, option: i32, value: i32) Error!void {
    try check(c.zmq_ctx_set(ctx, option, value));
}

fn check(rc: c_int) Error!void {
    if (rc == -1) return mapErrno();
}

fn mapErrno() Error {
    const errnum = c.zmq_errno();
    if (errnum == c.EAGAIN) return Error.Again;
    if (errnum == c.EADDRINUSE) return Error.AddressInUse;
    if (errnum == c.EADDRNOTAVAIL) return Error.AddressNotAvailable;
    if (errnum == c.EFAULT) return Error.Fault;
    if (errnum == c.EINVAL) return Error.Invalid;
    if (errnum == c.EINTR) return Error.Interrupted;
    if (errnum == c.EMSGSIZE) return Error.MessageTooLarge;
    if (errnum == c.ENOTCONN) return Error.NotConnected;
    if (errnum == c.ENOTSOCK) return Error.NoSocket;
    if (errnum == c.ENOTSUP) return Error.Unsupported;
    if (errnum == c.ETERM) return Error.ContextTerminated;
    if (errnum == c.ETIMEDOUT) return Error.TimedOut;
    if (errnum == c.EFSM) return Error.Protocol;
    if (errnum == c.EHOSTUNREACH) return Error.Unroutable;
    if (errnum == c.ENETUNREACH) return Error.HostUnreachable;
    return Error.Unknown;
}

fn dataPtr(data: []const u8) *const anyopaque {
    if (data.len == 0) return "";
    return data.ptr;
}

fn endpointPort(endpoint: []const u8) Error!u16 {
    const separator = std.mem.lastIndexOfScalar(u8, endpoint, ':') orelse return Error.Invalid;
    if (separator + 1 == endpoint.len) return Error.Invalid;
    return std.fmt.parseUnsigned(u16, endpoint[separator + 1 ..], 10) catch Error.Invalid;
}

fn msgDataSlice(msg: *c.zmq_msg_t, len: usize) Error![]const u8 {
    const raw = c.zmq_msg_data(msg);
    if (raw == null) {
        if (len == 0) return &.{};
        return Error.Fault;
    }
    const src: [*]const u8 = @ptrCast(raw.?);
    return src[0..len];
}

test {
    _ = c.ZMQ_VERSION;
}
