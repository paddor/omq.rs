using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;

namespace Omq;

/// Owns native I/O threads and the sockets created from them.
public sealed class Context : IDisposable
{
    private readonly object gate = new();
    private readonly List<Socket> sockets = [];
    private SafeContext? handle;
    private bool shutdown;
    /// Gets whether the context has been shut down or disposed.
    public bool Closed => handle is null;

    /// Creates a context with the requested number of native I/O threads.
    public Context(int ioThreads = 1)
    {
        if (ioThreads < 1) throw new ArgumentOutOfRangeException(nameof(ioThreads));
        IntPtr raw = Native.zmq_ctx_new();
        if (raw == IntPtr.Zero) throw new OmqException("ctx_new", Native.zmq_errno(), "context creation failed");
        handle = new SafeContext(raw);
        Errors.Check("ctx_set", Native.zmq_ctx_set(raw, 1, ioThreads));
    }

    private Context(IntPtr raw) => handle = new SafeContext(raw);
    /// Creates a context using the default construction style.
    public static Context Instance(int ioThreads = 1) => new(ioThreads);

    /// Imports a context shared through an OMQ share key.
    public static Context FromShareKey(ulong high, ulong low)
    {
        IntPtr raw = Native.omq_ctx_from_share_key(high, low);
        if (raw == IntPtr.Zero) throw new OmqException("ctx_from_share_key", Native.zmq_errno(), "context import failed");
        return new Context(raw);
    }

    /// Exports a key that can be used to share this context with another binding.
    public (ulong High, ulong Low) ShareKey()
    {
        lock (gate) { var h = Require(); Errors.Check("ctx_share_key", Native.omq_ctx_share_key(h, out ulong high, out ulong low)); return (high, low); }
    }

    /// Sets a native context option.
    public void SetOption(int option, int value) { lock (gate) Errors.Check("ctx_set", Native.zmq_ctx_set(Require(), option, value)); }
    /// Gets a native context option.
    public int GetOption(int option) { lock (gate) return Native.zmq_ctx_get(Require(), option); }

    /// Creates and configures a socket owned by this context.
    public Socket CreateSocket(SocketType type, SocketOptions options = default)
    {
        lock (gate)
        {
            IntPtr ctx = Require();
            IntPtr raw = Native.zmq_socket(ctx, (int)type);
            if (raw == IntPtr.Zero) { int e = Native.zmq_errno(); throw new OmqException("socket", e, "socket creation failed"); }
            var socket = new Socket(this, type, new SafeSocket(raw));
            try { options.Apply(socket); sockets.Add(socket); return socket; }
            catch { socket.Dispose(); throw; }
        }
    }

    internal void Remove(Socket socket) { lock (gate) sockets.Remove(socket); }
    private IntPtr Require() => handle?.DangerousGetHandle() is { } ptr && ptr != IntPtr.Zero ? ptr : throw new OmqClosedException();

    /// Closes sockets, wakes blocked operations, and releases the native context.
    public void Dispose()
    {
        lock (gate)
        {
            if (handle is null) return;
            if (!shutdown)
            {
                Errors.Check("ctx_shutdown", Native.zmq_ctx_shutdown(Require()));
                shutdown = true;
            }
            foreach (Socket socket in sockets.ToArray()) socket.Dispose();
            sockets.Clear();
            handle.Dispose();
            handle = null;
        }
        GC.SuppressFinalize(this);
    }

    /// Alias for <see cref="Dispose"/>.
    public void Destroy() => Dispose();

    /// Shuts down context I/O while leaving the managed context object usable for disposal.
    public void Shutdown()
    {
        lock (gate)
        {
            if (handle is null || shutdown) return;
            Errors.Check("ctx_shutdown", Native.zmq_ctx_shutdown(Require()));
            shutdown = true;
        }
    }

    internal sealed class SafeContext : SafeHandleZeroOrMinusOneIsInvalid
    {
        public SafeContext(IntPtr value) : base(true) => SetHandle(value);
        protected override bool ReleaseHandle() => Native.zmq_ctx_term(handle) == 0;
    }

    internal sealed class SafeSocket : SafeHandleZeroOrMinusOneIsInvalid
    {
        public SafeSocket(IntPtr value) : base(true) => SetHandle(value);
        protected override bool ReleaseHandle() => Native.zmq_close(handle) == 0;
    }
}
