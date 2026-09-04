using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Omq;

/// Thread-safe managed wrapper around one native OMQ socket.
public sealed class Socket : IDisposable
{
    private static long nextLockOrder;
    private readonly Context owner;
    private readonly object gate = new();
    private Context.SafeSocket? handle;
    /// Gets the socket pattern.
    public SocketType Type { get; }
    /// Gets whether the native socket has been closed.
    public bool Closed => handle is null;
    internal long LockOrder { get; } = Interlocked.Increment(ref nextLockOrder);
    /// Gets the native pollable file descriptor.
    public int FileDescriptor => GetInt32(SocketOption.FileDescriptor);

    internal Socket(Context owner, SocketType type, Context.SafeSocket handle) { this.owner = owner; Type = type; this.handle = handle; }
    private IntPtr Require() => handle?.DangerousGetHandle() is { } ptr && ptr != IntPtr.Zero ? ptr : throw new OmqClosedException();
    internal NativeLease AcquireNativeHandle()
    {
        System.Threading.Monitor.Enter(gate);
        try
        {
            var current = handle ?? throw new OmqClosedException();
            bool success = false;
            current.DangerousAddRef(ref success);
            if (!success) throw new OmqClosedException();
            return new NativeLease(current, current.DangerousGetHandle(), gate);
        }
        catch { System.Threading.Monitor.Exit(gate); throw; }
    }

    /// Sets an integer socket option.
    public void SetOption(int option, int value) => SetOption(option, BitConverter.GetBytes(value));
    /// Sets a 64-bit socket option.
    public void SetOption(int option, long value) => SetOption(option, BitConverter.GetBytes(value));
    /// Sets a raw socket option value.
    public void SetOption(int option, ReadOnlySpan<byte> value)
    {
        lock (gate) { IntPtr socket = Require(); byte[] copy = value.ToArray(); unsafe { fixed (byte* p = copy) Errors.Check("setsockopt", Native.zmq_setsockopt(socket, option, (IntPtr)p, (nuint)copy.Length)); } }
    }
    /// Sets a UTF-8 string socket option.
    public void SetOption(int option, string value) => SetOption(option, Encoding.UTF8.GetBytes(value));
    /// Gets an integer socket option.
    public int GetInt32(int option)
    {
        lock (gate) { int value = 0; nuint length = sizeof(int); unsafe { Errors.Check("getsockopt", Native.zmq_getsockopt(Require(), option, (IntPtr)(&value), ref length)); } return value; }
    }
    /// Gets a 64-bit socket option.
    public long GetInt64(int option)
    {
        lock (gate) { long value = 0; nuint length = sizeof(long); unsafe { Errors.Check("getsockopt", Native.zmq_getsockopt(Require(), option, (IntPtr)(&value), ref length)); } return value; }
    }
    /// Gets a variable-length socket option as bytes.
    public byte[] GetBytes(int option, int capacity = 1024)
    {
        lock (gate)
        {
            byte[] value = new byte[capacity]; nuint length = (nuint)value.Length;
            unsafe { fixed (byte* p = value) Errors.Check("getsockopt", Native.zmq_getsockopt(Require(), option, (IntPtr)p, ref length)); }
            return value[..checked((int)length)];
        }
    }
    /// Gets a UTF-8 string socket option.
    public string GetString(int option, int capacity = 1024)
    {
        lock (gate)
        {
            byte[] value = new byte[capacity]; nuint length = (nuint)value.Length;
            unsafe { fixed (byte* p = value) Errors.Check("getsockopt", Native.zmq_getsockopt(Require(), option, (IntPtr)p, ref length)); }
            return Encoding.UTF8.GetString(value, 0, checked((int)length)).TrimEnd('\0');
        }
    }
    /// Adds a subscription prefix.
    public void Subscribe(ReadOnlySpan<byte> prefix) => SetOption(SocketOption.Subscribe, prefix);
    /// Adds a UTF-8 subscription prefix.
    public void Subscribe(string prefix) => Subscribe(Encoding.UTF8.GetBytes(prefix));
    /// Removes a subscription prefix.
    public void Unsubscribe(ReadOnlySpan<byte> prefix) => SetOption(SocketOption.Unsubscribe, prefix);
    /// Removes a UTF-8 subscription prefix.
    public void Unsubscribe(string prefix) => Unsubscribe(Encoding.UTF8.GetBytes(prefix));
    /// Configures a PLAIN server accepting one fixed credential pair.
    public void ConfigurePlainServer(string username, string password)
    {
        lock (gate)
        {
            byte[] user = Encoding.UTF8.GetBytes(username);
            byte[] pass = Encoding.UTF8.GetBytes(password);
            unsafe
            {
                fixed (byte* userPtr = user)
                fixed (byte* passPtr = pass)
                    Errors.Check("plain_server", Native.omq_socket_set_plain_server_credentials(Require(), (IntPtr)userPtr, (nuint)user.Length, (IntPtr)passPtr, (nuint)pass.Length));
            }
        }
    }
    /// Configures PLAIN client credentials.
    public void ConfigurePlainClient(string username, string password) { SetOption(SocketOption.PlainUsername, username); SetOption(SocketOption.PlainPassword, password); }
    /// Enables CURVE server mode with its secret key.
    public void ConfigureCurveServer(string publicKey, string secretKey) { _ = publicKey; SetOption(SocketOption.CurveServer, 1); SetOption(SocketOption.CurveSecretKey, secretKey); }
    /// Configures CURVE client keys and the server key.
    public void ConfigureCurveClient(string publicKey, string secretKey, string serverPublicKey) { SetOption(SocketOption.CurvePublicKey, publicKey); SetOption(SocketOption.CurveSecretKey, secretKey); SetOption(SocketOption.CurveServerKey, serverPublicKey); }
    /// Creates a monitor for this socket.
    public Monitor Monitor(int events = 0xFFFF) => new(owner, this, events);
    /// Joins a RADIO/DISH group.
    public void Join(string group) => EndpointCall("join", group, NativeJoin);
    /// Leaves a RADIO/DISH group.
    public void Leave(string group) => EndpointCall("leave", group, NativeLeave);
    /// Binds and returns the effective endpoint.
    public string Bind(string endpoint)
    {
        EndpointCall("bind", endpoint, NativeBind);
        return GetString(SocketOption.LastEndpoint);
    }
    /// Connects to an endpoint.
    public void Connect(string endpoint) => EndpointCall("connect", endpoint, NativeConnect);
    /// Removes a binding.
    public void Unbind(string endpoint) => EndpointCall("unbind", endpoint, NativeUnbind);
    /// Removes a connection.
    public void Disconnect(string endpoint) => EndpointCall("disconnect", endpoint, NativeDisconnect);

    private delegate int EndpointFn(IntPtr socket, IntPtr text);
    private bool EndpointCall(string operation, string endpoint, EndpointFn fn)
    {
        lock (gate) { using var text = new Utf8(endpoint); Errors.Check(operation, fn(Require(), text.Pointer)); return true; }
    }
    private static int NativeBind(IntPtr s, IntPtr e) => Native.Bind(s, e);
    private static int NativeConnect(IntPtr s, IntPtr e) => Native.Connect(s, e);
    private static int NativeUnbind(IntPtr s, IntPtr e) => Native.Unbind(s, e);
    private static int NativeDisconnect(IntPtr s, IntPtr e) => Native.Disconnect(s, e);
    private static int NativeJoin(IntPtr s, IntPtr e) => NativeJoinImpl(s, e);
    private static int NativeLeave(IntPtr s, IntPtr e) => NativeLeaveImpl(s, e);
    [DllImport("omq_zmq", CallingConvention = CallingConvention.Cdecl, EntryPoint = "zmq_join")] private static extern int NativeJoinImpl(IntPtr s, IntPtr e);
    [DllImport("omq_zmq", CallingConvention = CallingConvention.Cdecl, EntryPoint = "zmq_leave")] private static extern int NativeLeaveImpl(IntPtr s, IntPtr e);

    /// Sends one frame, optionally marking it as multipart and/or non-blocking.
    public void Send(ReadOnlySpan<byte> data, bool more = false, bool dontWait = false)
    {
        lock (gate) { byte[] copy = data.ToArray(); unsafe { fixed (byte* p = copy) Errors.Check("send", Native.zmq_send(Require(), (IntPtr)p, (nuint)copy.Length, Flags(more, dontWait))); } }
    }
    /// Sends a UTF-8 frame.
    public void SendText(string text, bool more = false, bool dontWait = false) => Send(Encoding.UTF8.GetBytes(text), more, dontWait);
    /// Sends all frames in a message.
    public void Send(Message message, bool dontWait = false)
    {
        lock (gate)
        {
            for (int i = 0; i < message.Parts.Count; i++) SendPart(message.Parts[i], i + 1 < message.Parts.Count, dontWait, i == 0 ? message.RoutingId : 0);
        }
    }
    /// Sends copied multipart frames.
    public void SendMultipart(IEnumerable<ReadOnlyMemory<byte>> parts, bool dontWait = false) => Send(new Message(parts), dontWait);
    /// Sends copied multipart byte arrays.
    public void SendMultipart(params byte[][] parts) => Send(new Message(parts.Select(x => (ReadOnlyMemory<byte>)x)), false);
    private void SendPart(byte[] data, bool more, bool dontWait, uint routingId = 0)
    {
        Native.Message msg = new();
        Errors.Check("msg_init_size", Native.zmq_msg_init_size(ref msg, (nuint)data.Length));
        bool sent = false;
        try
        {
            Marshal.Copy(data, 0, Native.zmq_msg_data(ref msg), data.Length);
            if (routingId != 0) Errors.Check("msg_set_routing_id", Native.zmq_msg_set_routing_id(ref msg, routingId));
            Errors.Check("msg_send", Native.zmq_msg_send(ref msg, Require(), Flags(more, dontWait)));
            sent = true;
        }
        finally
        {
            if (!sent) Native.zmq_msg_close(ref msg);
        }
    }

    /// Receives one complete message, including all multipart frames.
    public Message Receive(bool dontWait = false)
    {
        lock (gate)
        {
            var parts = new List<byte[]>(); uint routingId = 0;
            int originalReceiveTimeout = 0;
            bool restoreReceiveTimeout = false;
            try
            {
                while (true)
                {
                    Native.Message msg = new();
                    Errors.Check("msg_init", Native.zmq_msg_init(ref msg));
                    try
                    {
                        Errors.Check("msg_recv", Native.zmq_msg_recv(ref msg, Require(), dontWait && parts.Count == 0 ? 1 : 0));
                        int size = checked((int)Native.zmq_msg_size(ref msg));
                        byte[] data = new byte[size];
                        if (size != 0) Marshal.Copy(Native.zmq_msg_data(ref msg), data, 0, size);
                        if (parts.Count == 0) routingId = Native.zmq_msg_routing_id(ref msg);
                        parts.Add(data);
                        if (Native.zmq_msg_more(ref msg) == 0) break;
                        if (!restoreReceiveTimeout)
                        {
                            originalReceiveTimeout = GetInt32(SocketOption.ReceiveTimeout);
                            if (originalReceiveTimeout != -1)
                            {
                                SetOption(SocketOption.ReceiveTimeout, -1);
                                restoreReceiveTimeout = true;
                            }
                        }
                    }
                    finally { Native.zmq_msg_close(ref msg); }
                }
                return new Message(parts.ToArray()) { RoutingId = routingId };
            }
            finally
            {
                if (restoreReceiveTimeout) SetOption(SocketOption.ReceiveTimeout, originalReceiveTimeout);
            }
        }
    }
    /// Receives a UTF-8 single-frame message.
    public string ReceiveText(bool dontWait = false) => Encoding.UTF8.GetString(Receive(dontWait).Data);
    /// Sends a UTF-8 string.
    public void SendString(string text, bool dontWait = false) => SendText(text, dontWait: dontWait);
    /// Receives a UTF-8 string.
    public string ReceiveString(bool dontWait = false) => ReceiveText(dontWait);
    /// Serializes and sends a JSON frame.
    public void SendJson<T>(T value, bool dontWait = false, bool more = false) => Send(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value)), more, dontWait);
    /// Receives and deserializes a JSON frame.
    public T? ReceiveJson<T>(bool dontWait = false) => JsonSerializer.Deserialize<T>(Receive(dontWait).Data);
    /// Polls this socket for readiness.
    public IReadOnlyList<PollResult> Poll(TimeSpan timeout, PollEvents events = PollEvents.Readable)
    {
        using var poller = new Poller();
        poller.Add(this, events);
        return poller.Wait(timeout);
    }
    /// Attempts a non-blocking frame send.
    public bool TrySend(ReadOnlySpan<byte> data)
    {
        try { Send(data, dontWait: true); return true; }
        catch (OmqAgainException) { return false; }
    }
    /// Attempts a non-blocking message send.
    public bool TrySend(Message message)
    {
        try { Send(message, dontWait: true); return true; }
        catch (OmqAgainException) { return false; }
    }
    /// Receives all frames as byte arrays.
    public IReadOnlyList<byte[]> ReceiveMultipart(bool dontWait = false) => Receive(dontWait).Parts;
    /// Attempts a non-blocking message receive.
    public bool TryReceive(out Message? message)
    {
        try { message = Receive(dontWait: true); return true; }
        catch (OmqAgainException) { message = null; return false; }
    }
    /// Receives one frame into a caller-provided buffer and returns the number of bytes copied.
    public int ReceiveInto(Span<byte> buffer, bool dontWait = false)
    {
        lock (gate) { unsafe { fixed (byte* p = buffer) { int n = Native.zmq_recv(Require(), (IntPtr)p, (nuint)buffer.Length, dontWait ? 1 : 0); Errors.Check("recv", n); return Math.Min(n, buffer.Length); } } }
    }
    private static int Flags(bool more, bool dontWait) => (more ? 2 : 0) | (dontWait ? 1 : 0);

    /// Closes the socket and releases its native handle.
    public void Dispose()
    {
        lock (gate) { if (handle is null) return; handle.Dispose(); handle = null; }
        owner.Remove(this); GC.SuppressFinalize(this);
    }

    internal void EnableMonitor(string endpoint, int events)
    {
        lock (gate) { IntPtr text = Marshal.StringToCoTaskMemUTF8(endpoint); try { Errors.Check("socket_monitor", Native.zmq_socket_monitor(Require(), text, events)); } finally { Marshal.FreeCoTaskMem(text); } }
    }
    internal void DisableMonitor()
    {
        lock (gate) { Errors.Check("socket_monitor_stop", Native.zmq_socket_monitor(Require(), IntPtr.Zero, 0)); }
    }

    private sealed class Utf8 : IDisposable { public IntPtr Pointer { get; } public Utf8(string value) => Pointer = Marshal.StringToCoTaskMemUTF8(value); public void Dispose() => Marshal.FreeCoTaskMem(Pointer); }

    internal sealed class NativeLease : IDisposable
    {
        private Context.SafeSocket? handle;
        private object? gate;
        internal IntPtr Pointer { get; }
        internal NativeLease(Context.SafeSocket handle, IntPtr pointer, object gate) { this.handle = handle; this.gate = gate; Pointer = pointer; }
        public void Dispose()
        {
            var current = Interlocked.Exchange(ref handle, null);
            if (current is null) return;
            current.DangerousRelease();
            System.Threading.Monitor.Exit(Interlocked.Exchange(ref gate, null)!);
        }
    }

    internal sealed class NativeLeaseSet : IDisposable
    {
        private readonly Dictionary<Socket, NativeLease> leases = [];

        internal NativeLeaseSet(IEnumerable<Socket?> sockets)
        {
            var acquired = new List<NativeLease>();
            try
            {
                foreach (var socket in sockets.OfType<Socket>().Distinct().OrderBy(socket => socket.LockOrder))
                {
                    NativeLease lease = socket.AcquireNativeHandle();
                    acquired.Add(lease);
                    leases.Add(socket, lease);
                }
            }
            catch
            {
                foreach (var lease in acquired.AsEnumerable().Reverse()) lease.Dispose();
                throw;
            }
        }

        internal IntPtr this[Socket socket] => leases[socket].Pointer;
        internal IntPtr Get(Socket? socket) => socket is null ? IntPtr.Zero : this[socket];

        public void Dispose()
        {
            foreach (var lease in leases.OrderByDescending(item => item.Key.LockOrder))
                lease.Value.Dispose();
        }
    }
}
