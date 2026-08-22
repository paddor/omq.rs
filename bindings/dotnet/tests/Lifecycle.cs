using Omq;

static void Check(bool value, string message)
{
    if (!value) throw new Exception(message);
}

static async Task AsyncSendUnderHwmIsBounded()
{
    using var context = new Context();
    using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0, ReceiveHwm = 1 });
    using var push = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0, SendHwm = 1, Immediate = true });
    string endpoint = pull.Bind("tcp://127.0.0.1:0");
    push.Connect(endpoint);
    await Task.Delay(50);
    byte[] payload = new byte[4096];
    int filled = 0;
    while (push.TrySend(payload)) filled++;
    Check(filled > 0, "HWM fixture did not fill");

    using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
    Task pending = push.SendAsync(new Message([payload, payload]), cancellation.Token);
    try { await pending.WaitAsync(TimeSpan.FromSeconds(2)); }
    catch (OperationCanceledException) { }
}

static async Task ShutdownAndCancellationBoundPoll()
{
    using var context = new Context();
    using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0 });
    using var poller = new Poller();
    poller.Add(pull);
    using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
    Task<IReadOnlyList<PollResult>> pending = poller.WaitAsync(TimeSpan.FromSeconds(10), cancellation.Token);
    await Task.Delay(50);
    context.Shutdown();
    await pending.WaitAsync(TimeSpan.FromSeconds(2));
}

static async Task MonitorStopWakesReceive()
{
    using var context = new Context();
    using var socket = context.CreateSocket(SocketType.Pair, new SocketOptions { Linger = 0 });
    using var monitor = socket.Monitor();
    Task<MonitorEvent> pending = monitor.ReceiveAsync();
    await Task.Delay(30);
    monitor.Dispose();
    try { await pending.WaitAsync(TimeSpan.FromSeconds(2)); throw new Exception("monitor stop did not interrupt receive"); }
    catch (OperationCanceledException) { }
}

static async Task ConnectBeforeBind()
{
    using var context = new Context();
    using var push = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0, SendTimeout = TimeSpan.FromSeconds(2) });
    using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0, ReceiveTimeout = TimeSpan.FromSeconds(2) });
    string endpoint = "tcp://127.0.0.1:38291";
    push.Connect(endpoint);
    await Task.Delay(30);
    pull.Bind(endpoint);
    push.SendText("late-bind");
    Check(pull.ReceiveText() == "late-bind", "connect-before-bind mismatch");
}

static async Task TryReceivePreservesMultipartAtomicity()
{
    using var context = new Context();
    using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0, ReceiveTimeout = TimeSpan.FromSeconds(2) });
    using var push = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0, SendTimeout = TimeSpan.FromSeconds(2) });
    string endpoint = pull.Bind("tcp://127.0.0.1:0");
    push.Connect(endpoint);
    await Task.Delay(50);

    Task sender = Task.Run(async () =>
    {
        push.SendText("first", more: true);
        await Task.Delay(100);
        push.SendText("second");
    });

    Message? message = null;
    DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
    while (DateTime.UtcNow < deadline && !pull.TryReceive(out message)) await Task.Delay(1);
    await sender.WaitAsync(TimeSpan.FromSeconds(2));

    Message received = message ?? throw new Exception("multipart TryReceive timed out");
    Check(received.PartCount == 2, "multipart TryReceive dropped a frame");
    Check(received[0].SequenceEqual("first"u8.ToArray()), "multipart first frame mismatch");
    Check(received[1].SequenceEqual("second"u8.ToArray()), "multipart second frame mismatch");
}

static void RepeatedCreateDispose()
{
    for (int i = 0; i < 100; i++)
    {
        using var context = new Context();
        using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0 });
        using var push = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0 });
        string endpoint = pull.Bind($"inproc://dotnet-lifecycle-{Environment.ProcessId}-{i}");
        push.Connect(endpoint);
        push.SendText("cycle");
        Check(pull.ReceiveText() == "cycle", $"cycle {i} mismatch");
    }
}

await AsyncSendUnderHwmIsBounded();
await ShutdownAndCancellationBoundPoll();
await MonitorStopWakesReceive();
await ConnectBeforeBind();
await TryReceivePreservesMultipartAtomicity();
RepeatedCreateDispose();
Console.WriteLine("OMQ.Net lifecycle: PASS");
