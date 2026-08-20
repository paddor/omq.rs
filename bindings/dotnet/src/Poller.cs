namespace Omq;

[Flags]
public enum PollEvents { None = 0, Readable = 1, Writable = 2, Error = 4 }
public readonly record struct PollResult(Socket Socket, PollEvents Events);

public sealed class Poller : IDisposable
{
    private readonly object gate = new();
    private readonly List<(Socket Socket, PollEvents Events)> entries = [];
    public IReadOnlyList<Socket> Sockets { get { lock (gate) return entries.Select(x => x.Socket).ToArray(); } }
    public void Add(Socket socket, PollEvents events = PollEvents.Readable) { lock (gate) entries.Add((socket, events)); }
    public bool Remove(Socket socket) { lock (gate) return entries.RemoveAll(x => ReferenceEquals(x.Socket, socket)) != 0; }

    public IReadOnlyList<PollResult> Wait() => Wait(Timeout.InfiniteTimeSpan);

    public IReadOnlyList<PollResult> Wait(TimeSpan timeout)
    {
        (Socket Socket, PollEvents Events)[] snapshot;
        lock (gate) snapshot = entries.ToArray();
        if (snapshot.Length == 0)
        {
            if (timeout > TimeSpan.Zero) Thread.Sleep(timeout);
            return [];
        }
        using var leases = new LeaseSet(snapshot.Select(x => x.Socket));
        var items = new Native.PollItem[snapshot.Length];
        for (int i = 0; i < items.Length; i++)
            items[i] = new Native.PollItem { Socket = leases[i].Pointer, FileDescriptor = -1, Events = (short)snapshot[i].Events };
        int milliseconds = checked((int)Math.Clamp(timeout.TotalMilliseconds, -1, int.MaxValue));
        unsafe { fixed (Native.PollItem* p = items) Errors.Check("poll", Native.zmq_poll((IntPtr)p, items.Length, milliseconds)); }
        var ready = new List<PollResult>();
        for (int i = 0; i < items.Length; i++)
        {
            PollEvents actual = (PollEvents)items[i].Revents & (PollEvents.Readable | PollEvents.Writable | PollEvents.Error);
            if (actual != PollEvents.None) ready.Add(new PollResult(snapshot[i].Socket, actual));
        }
        return ready;
    }

    public Task<IReadOnlyList<PollResult>> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        Task.Run(() => WaitCancellable(timeout, cancellationToken), cancellationToken);

    private IReadOnlyList<PollResult> WaitCancellable(TimeSpan timeout, CancellationToken token)
    {
        DateTime deadline = timeout == default ? DateTime.MaxValue : DateTime.UtcNow + timeout;
        while (true)
        {
            token.ThrowIfCancellationRequested();
            TimeSpan slice = deadline == DateTime.MaxValue ? TimeSpan.FromMilliseconds(100) : deadline - DateTime.UtcNow;
            if (slice <= TimeSpan.Zero) return [];
            var ready = Wait(slice > TimeSpan.FromMilliseconds(100) ? TimeSpan.FromMilliseconds(100) : slice);
            if (ready.Count != 0 || DateTime.UtcNow >= deadline) return ready;
        }
    }

    public void Dispose() { lock (gate) entries.Clear(); }

    private sealed class LeaseSet : IDisposable
    {
        private readonly Socket.NativeLease[] leases;
        internal LeaseSet(IEnumerable<Socket> sockets)
        {
            var acquired = new List<Socket.NativeLease>();
            try
            {
                foreach (var socket in sockets) acquired.Add(socket.AcquireNativeHandle());
                leases = acquired.ToArray();
            }
            catch
            {
                foreach (var lease in acquired) lease.Dispose();
                throw;
            }
        }
        internal Socket.NativeLease this[int index] => leases[index];
        public void Dispose() { foreach (var lease in leases) lease.Dispose(); }
    }
}
