using System.Diagnostics;
using Omq;

internal static class Program
{
    private const int ReportIntervalSeconds = 10;
    private static readonly TimeSpan ExchangeTimeout = TimeSpan.FromSeconds(
        ReadPositiveDouble("OMQ_DOTNET_SOAK_EXCHANGE_TIMEOUT_SECS", "OMQ_SOAK_EXCHANGE_TIMEOUT_SECS", 120));
    private static readonly byte[] LargePayload = Enumerable.Repeat((byte)0xA5, 1024 * 1024).ToArray();
    private static readonly SocketOptions Options = new()
    {
        Linger = 0,
        SendTimeout = ExchangeTimeout,
        ReceiveTimeout = ExchangeTimeout,
    };

    public static void Main()
    {
        WarmUpRuntime();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Thread.Sleep(100);

        double durationSeconds = ReadPositiveDouble("OMQ_DOTNET_SOAK_DURATION_SECS", "OMQ_SOAK_DURATION_SECS", 60);
        var timer = Stopwatch.StartNew();
        TimeSpan duration = TimeSpan.FromSeconds(durationSeconds);
        TimeSpan nextReport = TimeSpan.FromSeconds(ReportIntervalSeconds);
        ResourceSample baseline = SampleResources(timer.Elapsed);
        DumpFileDescriptors("baseline");
        var samples = new List<ResourceSample>();
        var counters = new Dictionary<string, long>();

        {
            using var context = new Context(Math.Max(1, ReadPositiveInt("OMQ_DOTNET_SOAK_IO_THREADS", 2)));
            var tcp = OpenPushPull(context, "tcp://127.0.0.1:0");
            using var ipc = OpenPushPull(context, $"ipc://@omq-dotnet-soak-{Environment.ProcessId}");
            using var inproc = OpenPushPull(context, $"inproc://omq-dotnet-soak-{Environment.ProcessId}");
            using var lz4 = OpenPushPull(context, "lz4+tcp://127.0.0.1:0");
            using var zstd = OpenPushPull(context, "zstd+tcp://127.0.0.1:0");
            using var plain = OpenPlainPushPull(context);
            using var curve = OpenCurvePushPull(context);
            using var reqRep = OpenPair(context, SocketType.Req, SocketType.Rep, "tcp://127.0.0.1:0");
            using var pair = OpenPair(context, SocketType.Pair, SocketType.Pair, "tcp://127.0.0.1:0");
            using var pubSub = OpenPubSub(context);
            int cycle = 0;
            bool exercisedReconnect = false;

            try
            {
                while (timer.Elapsed < duration)
                {
                    cycle++;
                    TraceStage(cycle, "tcp");
                    ExerciseResilientTcpPushPull(context, ref tcp, cycle, counters);
                    TraceStage(cycle, "ipc");
                    ExercisePushPull(ipc, cycle, counters, "ipc");
                    TraceStage(cycle, "inproc");
                    ExercisePushPull(inproc, cycle, counters, "inproc");
                    TraceStage(cycle, "lz4");
                    ExercisePushPull(lz4, cycle, counters, "lz4");
                    TraceStage(cycle, "zstd");
                    ExercisePushPull(zstd, cycle, counters, "zstd");
                    TraceStage(cycle, "plain");
                    ExercisePushPull(plain, cycle, counters, "plain");
                    TraceStage(cycle, "curve");
                    ExercisePushPull(curve, cycle, counters, "curve");
                    TraceStage(cycle, "reqrep");
                    ExerciseReqRep(reqRep, cycle, counters);
                    TraceStage(cycle, "pair");
                    ExercisePair(pair, cycle, counters);
                    TraceStage(cycle, "pubsub");
                    ExercisePubSub(pubSub, cycle, counters);
                    if (cycle % 10 == 0) ExerciseContextChurn(cycle, counters);
                    if (!exercisedReconnect && timer.Elapsed >= TimeSpan.FromSeconds(1))
                    {
                        ExerciseReconnect(counters);
                        exercisedReconnect = true;
                    }

                    if (timer.Elapsed < nextReport) continue;
                    ResourceSample current = SampleResources(timer.Elapsed);
                    samples.Add(current);
                    Console.Error.WriteLine(FormatReport(current, counters));
                    nextReport = timer.Elapsed + TimeSpan.FromSeconds(ReportIntervalSeconds);
                }
            }
            finally
            {
                tcp.Dispose();
            }
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Thread.Sleep(100);

        string[] required = ["tcp", "ipc", "inproc", "lz4", "zstd", "plain", "curve", "reqrep", "pair", "pubsub", "fanout"];
        foreach (string name in required) Check(Counter(counters, name) > 0, $"{name} made no progress");
        ResourceSample final = SampleResources(timer.Elapsed);
        DumpFileDescriptors("final");
        AssertResourceGrowth(baseline, final, samples);
        Console.WriteLine("OMQ.Net soak: PASS");
    }

    private static void WarmUpRuntime()
    {
        _ = Curve.GenerateKeyPair();
        using var context = new Context();
        using var pair = OpenPushPull(context, "tcp://127.0.0.1:0");
        using var poller = new Poller();
        poller.Add(pair.Receiver);
        pair.Sender.SendText("warmup");
        Check(poller.Wait(TimeSpan.FromSeconds(5)).Count == 1, "warmup poller stalled");
        Check(pair.Receiver.ReceiveText() == "warmup", "warmup message mismatch");
        pair.Sender.Send(LargePayload);
        Check(pair.Receiver.Receive().Data.SequenceEqual(LargePayload), "warmup large payload mismatch");
    }

    private static SocketPair OpenPushPull(Context context, string endpoint)
    {
        Socket receiver = context.CreateSocket(SocketType.Pull, Options);
        Socket sender = context.CreateSocket(SocketType.Push, Options);
        try
        {
            string resolved = receiver.Bind(endpoint);
            sender.Connect(resolved);
            return new SocketPair(sender, receiver);
        }
        catch
        {
            sender.Dispose();
            receiver.Dispose();
            throw;
        }
    }

    private static SocketPair OpenPlainPushPull(Context context)
    {
        Socket receiver = context.CreateSocket(SocketType.Pull, Options);
        Socket sender = context.CreateSocket(SocketType.Push, Options);
        try
        {
            receiver.ConfigurePlainServer("soak", "secret");
            sender.ConfigurePlainClient("soak", "secret");
            string endpoint = receiver.Bind("tcp://127.0.0.1:0");
            sender.Connect(endpoint);
            return new SocketPair(sender, receiver);
        }
        catch
        {
            sender.Dispose();
            receiver.Dispose();
            throw;
        }
    }

    private static SocketPair OpenCurvePushPull(Context context)
    {
        CurveKeyPair serverKeys = Curve.GenerateKeyPair();
        CurveKeyPair clientKeys = Curve.GenerateKeyPair();
        Socket receiver = context.CreateSocket(SocketType.Pull, Options);
        Socket sender = context.CreateSocket(SocketType.Push, Options);
        try
        {
            receiver.ConfigureCurveServer(serverKeys.PublicKey, serverKeys.SecretKey);
            sender.ConfigureCurveClient(clientKeys.PublicKey, clientKeys.SecretKey, serverKeys.PublicKey);
            string endpoint = receiver.Bind("tcp://127.0.0.1:0");
            sender.Connect(endpoint);
            return new SocketPair(sender, receiver);
        }
        catch
        {
            sender.Dispose();
            receiver.Dispose();
            throw;
        }
    }

    private static SocketPair OpenPair(Context context, SocketType senderType, SocketType receiverType, string endpoint)
    {
        Socket receiver = context.CreateSocket(receiverType, Options);
        Socket sender = context.CreateSocket(senderType, Options);
        try
        {
            string resolved = receiver.Bind(endpoint);
            sender.Connect(resolved);
            return new SocketPair(sender, receiver);
        }
        catch
        {
            sender.Dispose();
            receiver.Dispose();
            throw;
        }
    }

    private static PubSub OpenPubSub(Context context)
    {
        Socket publisher = context.CreateSocket(SocketType.Pub, Options);
        Socket first = context.CreateSocket(SocketType.Sub, Options);
        Socket second = context.CreateSocket(SocketType.Sub, Options);
        try
        {
            string endpoint = publisher.Bind("tcp://127.0.0.1:0");
            first.Subscribe("soak.");
            second.Subscribe("soak.");
            first.Connect(endpoint);
            second.Connect(endpoint);
            Thread.Sleep(100);
            var sockets = new PubSub(publisher, first, second);
            WaitForPubSub(sockets);
            return sockets;
        }
        catch
        {
            second.Dispose();
            first.Dispose();
            publisher.Dispose();
            throw;
        }
    }

    private static void ExercisePushPull(SocketPair pair, int cycle, Dictionary<string, long> counters, string name, bool poll = false)
    {
        byte[] sequence = BitConverter.GetBytes(cycle);
        pair.Sender.Send(new Message([System.Text.Encoding.UTF8.GetBytes(name), sequence]));
        bool pollHit = false;
        if (poll)
        {
            pollHit = WaitReadable(pair.Receiver, TimeSpan.FromMilliseconds(100));
            if (pollHit) Increment(counters, "poller");
        }
        Message received;
        try
        {
            received = ReceiveWithin(pair.Receiver, ExchangeTimeout, $"{name} receive timed out");
        }
        catch (InvalidOperationException ex)
        {
            string progress = string.Join(' ', counters.OrderBy(x => x.Key).Select(x => $"{x.Key}={x.Value}"));
            throw new InvalidOperationException(
                $"{name} receive timed out cycle={cycle} poll_hit={pollHit} sender_events={pair.Sender.GetInt32(SocketOption.Events)} receiver_events={pair.Receiver.GetInt32(SocketOption.Events)} counters={progress}",
                ex);
        }
        Check(received.PartCount == 2, $"{name} multipart count mismatch");
        Check(received[0].SequenceEqual(System.Text.Encoding.UTF8.GetBytes(name)), $"{name} label mismatch");
        Check(received[1].SequenceEqual(sequence), $"{name} sequence mismatch");
        Increment(counters, name);
        Increment(counters, "multipart");

        if (name != "tcp" || cycle % 25 != 0) return;
        pair.Sender.Send(LargePayload);
        Check(pair.Receiver.Receive().Data.SequenceEqual(LargePayload), $"{name} large payload mismatch");
        Increment(counters, "large");
    }

    private static void ExerciseResilientTcpPushPull(
        Context context,
        ref SocketPair pair,
        int cycle,
        Dictionary<string, long> counters)
    {
        try
        {
            ExercisePushPull(pair, cycle, counters, "tcp", poll: true);
        }
        catch (InvalidOperationException ex) when (
            ex.Message.StartsWith("tcp receive timed out", StringComparison.Ordinal))
        {
            Increment(counters, "tcp-timeouts");
            Console.Error.WriteLine($"[dotnet-soak] tcp exchange timed out; reopening pair: {ex.Message}");
            pair.Dispose();
            pair = OpenPushPull(context, "tcp://127.0.0.1:0");
        }
    }

    private static Message ReceiveWithin(Socket socket, TimeSpan timeout, string failure)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (socket.TryReceive(out Message? message) && message is not null) return message;
            Thread.Sleep(1);
        }
        throw new InvalidOperationException(failure);
    }

    private static bool WaitReadable(Socket socket, TimeSpan timeout)
    {
        using var poller = new Poller();
        poller.Add(socket);
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            TimeSpan remaining = deadline - DateTime.UtcNow;
            TimeSpan slice = remaining < TimeSpan.FromMilliseconds(250) ? remaining : TimeSpan.FromMilliseconds(250);
            if (poller.Wait(slice).Any(result => result.Events.HasFlag(PollEvents.Readable))) return true;
        }
        return false;
    }

    private static void ExerciseReqRep(SocketPair pair, int cycle, Dictionary<string, long> counters)
    {
        pair.Sender.SendText($"req-{cycle}");
        Check(pair.Receiver.ReceiveText() == $"req-{cycle}", "REQ/REP request mismatch");
        pair.Receiver.SendText($"rep-{cycle}");
        Check(pair.Sender.ReceiveText() == $"rep-{cycle}", "REQ/REP reply mismatch");
        Increment(counters, "reqrep");
    }

    private static void ExercisePair(SocketPair pair, int cycle, Dictionary<string, long> counters)
    {
        pair.Sender.SendText($"a-{cycle}");
        Check(pair.Receiver.ReceiveText() == $"a-{cycle}", "PAIR forward mismatch");
        pair.Receiver.SendText($"b-{cycle}");
        Check(pair.Sender.ReceiveText() == $"b-{cycle}", "PAIR reverse mismatch");
        Increment(counters, "pair", 2);
    }

    private static void ExercisePubSub(PubSub sockets, int cycle, Dictionary<string, long> counters)
    {
        string message = $"soak.{cycle}";
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        bool first = false;
        bool second = false;
        while (DateTime.UtcNow < deadline && (!first || !second))
        {
            for (int i = 0; i < 8; i++) sockets.Publisher.SendText(message);
            DateTime pollUntil = DateTime.UtcNow + TimeSpan.FromMilliseconds(50);
            while (DateTime.UtcNow < pollUntil && (!first || !second))
            {
                if (!first) first = TryReceiveTextPrefix(sockets.First, "soak.");
                if (!second) second = TryReceiveTextPrefix(sockets.Second, "soak.");
                if (!first || !second) Thread.Sleep(1);
            }
        }
        if (first) Increment(counters, "pubsub");
        if (second) Increment(counters, "pubsub");
        if (first && second) Increment(counters, "fanout");
    }

    private static void WaitForPubSub(PubSub sockets)
    {
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        int attempt = 0;
        while (DateTime.UtcNow < deadline)
        {
            string message = $"soak.ready.{attempt++}";
            sockets.Publisher.SendText(message);
            DateTime pollUntil = DateTime.UtcNow + TimeSpan.FromMilliseconds(100);
            bool first = false;
            bool second = false;
            while (DateTime.UtcNow < pollUntil && (!first || !second))
            {
                if (!first) first = TryReceiveText(sockets.First, message);
                if (!second) second = TryReceiveText(sockets.Second, message);
                if (!first || !second) Thread.Sleep(1);
            }
            if (first && second) return;
        }
        throw new InvalidOperationException("PUB/SUB readiness timed out");
    }

    private static bool TryReceiveText(Socket socket, string expected)
    {
        while (socket.TryReceive(out Message? message))
        {
            if (message is not null && message.ToString() == expected) return true;
        }
        return false;
    }

    private static bool TryReceiveTextPrefix(Socket socket, string prefix)
    {
        while (socket.TryReceive(out Message? message))
        {
            if (message is not null && message.ToString().StartsWith(prefix, StringComparison.Ordinal)) return true;
        }
        return false;
    }

    private static void ExerciseContextChurn(int cycle, Dictionary<string, long> counters)
    {
        using var context = new Context();
        using var pair = OpenPushPull(context, $"inproc://omq-dotnet-churn-{Environment.ProcessId}-{cycle}");
        pair.Sender.SendText("churn");
        Check(pair.Receiver.ReceiveText() == "churn", "context churn mismatch");
        Increment(counters, "context-churn");
    }

    private static void ExerciseReconnect(Dictionary<string, long> counters)
    {
        using var context = new Context();
        Socket receiver = context.CreateSocket(SocketType.Pull, Options);
        using Socket sender = context.CreateSocket(SocketType.Push, Options with
        {
            Immediate = true,
            ReconnectInterval = 10,
            ReconnectIntervalMax = 100,
        });
        string endpoint = receiver.Bind("tcp://127.0.0.1:0");
        sender.Connect(endpoint);
        sender.SendText("before");
        Check(receiver.ReceiveText() == "before", "pre-reconnect mismatch");
        receiver.Dispose();

        receiver = context.CreateSocket(SocketType.Pull, Options);
        try
        {
            receiver.Bind(endpoint);
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            Message? received = null;
            while (DateTime.UtcNow < deadline && received is null)
            {
                sender.TrySend(System.Text.Encoding.UTF8.GetBytes("after"));
                receiver.TryReceive(out received);
                if (received is null) Thread.Sleep(10);
            }
            Check(received?.ToString() == "after", "post-reconnect mismatch");
            Increment(counters, "reconnect");
        }
        finally
        {
            receiver.Dispose();
        }
    }

    private static ResourceSample SampleResources(TimeSpan elapsed)
    {
        using Process process = Process.GetCurrentProcess();
        int fds = Directory.Exists("/proc/self/fd") ? Directory.EnumerateFileSystemEntries("/proc/self/fd").Count() : 0;
        return new ResourceSample(elapsed, process.WorkingSet64, fds, process.Threads.Count, GC.GetTotalMemory(false));
    }

    private static void AssertResourceGrowth(ResourceSample baseline, ResourceSample final, List<ResourceSample> samples)
    {
        Check(final.FileDescriptors - baseline.FileDescriptors <= 16, $"FD leak: {baseline.FileDescriptors} -> {final.FileDescriptors}");
        Check(final.Threads - baseline.Threads <= 8, $"thread leak: {baseline.Threads} -> {final.Threads}");
        if (samples.Count < 12) return;

        ResourceSample[] warm = samples.Skip(samples.Count / 5).ToArray();
        long rssBaseline = (long)warm.Take(Math.Max(1, warm.Length / 10)).Average(x => x.RssBytes);
        long rssTail = warm.TakeLast(Math.Max(1, warm.Length / 5)).Max(x => x.RssBytes);
        long growth = rssTail - rssBaseline;
        Check(growth < 64L * 1024 * 1024 || (double)growth / rssBaseline < 0.25,
            $"RSS leak: baseline={rssBaseline} tail={rssTail} growth={growth}");
    }

    private static string FormatReport(ResourceSample sample, Dictionary<string, long> counters)
    {
        string progress = string.Join(' ', counters.OrderBy(x => x.Key).Select(x => $"{x.Key}={x.Value}"));
        return $"[dotnet-soak] {sample.Elapsed.TotalSeconds:F0}s {progress} rss={sample.RssBytes / 1048576.0:F1}MB heap={sample.HeapBytes / 1048576.0:F1}MB fds={sample.FileDescriptors} threads={sample.Threads}";
    }

    private static void DumpFileDescriptors(string label)
    {
        if (Environment.GetEnvironmentVariable("OMQ_SOAK_TRACE") != "1" || !Directory.Exists("/proc/self/fd")) return;
        foreach (FileSystemInfo entry in new DirectoryInfo("/proc/self/fd").EnumerateFileSystemInfos().OrderBy(entry => entry.Name))
            Console.Error.WriteLine($"[dotnet-soak-fd] {label} {entry.Name} -> {entry.LinkTarget}");
    }

    private static double ReadPositiveDouble(string primary, string secondary, double fallback)
    {
        string? raw = Environment.GetEnvironmentVariable(primary) ?? Environment.GetEnvironmentVariable(secondary);
        double value = raw is null ? fallback : double.Parse(raw, System.Globalization.CultureInfo.InvariantCulture);
        if (value <= 0) throw new ArgumentOutOfRangeException(primary);
        return value;
    }

    private static int ReadPositiveInt(string name, int fallback)
    {
        string? raw = Environment.GetEnvironmentVariable(name);
        int value = raw is null ? fallback : int.Parse(raw, System.Globalization.CultureInfo.InvariantCulture);
        if (value <= 0) throw new ArgumentOutOfRangeException(name);
        return value;
    }

    private static void Increment(Dictionary<string, long> counters, string name, long amount = 1) => counters[name] = Counter(counters, name) + amount;
    private static long Counter(Dictionary<string, long> counters, string name) => counters.GetValueOrDefault(name);
    private static void TraceStage(int cycle, string stage)
    {
        if (Environment.GetEnvironmentVariable("OMQ_SOAK_TRACE") == "1")
            Console.Error.WriteLine($"[dotnet-soak] cycle={cycle} stage={stage}");
    }
    private static void Check(bool condition, string message) { if (!condition) throw new InvalidOperationException(message); }

    private sealed class SocketPair(Socket sender, Socket receiver) : IDisposable
    {
        public Socket Sender { get; } = sender;
        public Socket Receiver { get; } = receiver;
        public void Dispose() { Sender.Dispose(); Receiver.Dispose(); }
    }

    private sealed class PubSub(Socket publisher, Socket first, Socket second) : IDisposable
    {
        public Socket Publisher { get; } = publisher;
        public Socket First { get; } = first;
        public Socket Second { get; } = second;
        public void Dispose() { Second.Dispose(); First.Dispose(); Publisher.Dispose(); }
    }

    private readonly record struct ResourceSample(TimeSpan Elapsed, long RssBytes, int FileDescriptors, int Threads, long HeapBytes);
}
