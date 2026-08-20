using System.Buffers.Binary;

namespace Omq;

public enum MonitorEventType : ushort
{
    Connected = 0x0001, ConnectDelayed = 0x0002, ConnectRetried = 0x0004,
    Listening = 0x0008, BindFailed = 0x0010, Accepted = 0x0020,
    AcceptFailed = 0x0040, Closed = 0x0080, CloseFailed = 0x0100,
    Disconnected = 0x0200, MonitorStopped = 0x0400,
    HandshakeFailed = 0x0800, HandshakeSucceeded = 0x1000
}

public readonly record struct MonitorEvent(MonitorEventType Type, uint Value, string Endpoint);

public sealed class Monitor : IDisposable
{
    private readonly Socket source;
    private readonly Socket reader;
    private bool stopped;
    private readonly CancellationTokenSource stopping = new();

    internal Monitor(Context context, Socket source, int events)
    {
        this.source = source;
        string endpoint = $"inproc://omq-dotnet-monitor-{Guid.NewGuid():N}";
        source.EnableMonitor(endpoint, events);
        reader = context.CreateSocket(SocketType.Pair, new SocketOptions { Linger = 0 });
        reader.Connect(endpoint);
    }

    public MonitorEvent Receive(bool dontWait = false)
    {
        return Parse(reader.Receive(dontWait));
    }

    private static MonitorEvent Parse(Message message)
    {
        if (message.Parts.Count < 2 || message.Parts[0].Length < 6) throw new OmqException("monitor", 0, "malformed monitor event");
        ReadOnlySpan<byte> header = message.Parts[0];
        var type = (MonitorEventType)BinaryPrimitives.ReadUInt16LittleEndian(header);
        uint value = BinaryPrimitives.ReadUInt32LittleEndian(header[2..]);
        return new MonitorEvent(type, value, System.Text.Encoding.UTF8.GetString(message.Parts[1]));
    }

    public async Task<MonitorEvent> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stopping.Token);
        var poller = new Poller(); poller.Add(reader, PollEvents.Readable);
        Message? message;
        while (!reader.TryReceive(out message))
        {
            linked.Token.ThrowIfCancellationRequested();
            await poller.WaitAsync(TimeSpan.FromMilliseconds(100), linked.Token).ConfigureAwait(false);
        }
        return Parse(message!);
    }

    public void Dispose()
    {
        if (stopped) return;
        stopped = true;
        stopping.Cancel();
        try { source.DisableMonitor(); } catch (OmqClosedException) { }
        reader.Dispose();
        stopping.Dispose();
        GC.SuppressFinalize(this);
    }
}
