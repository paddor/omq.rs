using System.Diagnostics;
using System.Text.Json;
using NetMQ;
using NetMQ.Sockets;
using Omq;

if (args.Length != 7) throw new ArgumentException("mode impl role endpoint size seconds warmup_seconds");
string mode = args[0], impl = args[1], role = args[2], endpoint = args[3];
int size = int.Parse(args[4]); double seconds = double.Parse(args[5]); double warmup = double.Parse(args[6]);
byte[] payload = new byte[size]; for (int i = 0; i < payload.Length; i++) payload[i] = (byte)i;

if (impl == "omq") RunOmq();
else if (impl == "omq-async") RunOmqAsync();
else if (impl == "netmq") RunNetMq();
else if (impl == "netmq-async") RunNetMqAsync();
else throw new ArgumentException($"unknown implementation {impl}");

void Ready() { Console.WriteLine($"READY {endpoint}"); Console.Out.Flush(); }
void Result(long count, double elapsed, double p50 = 0)
{
    var row = new { impl, pattern = mode, size, messages_per_second = count / elapsed, megabytes_per_second = count * (double)size / elapsed / 1_000_000, p50_us = p50 };
    Console.WriteLine($"RESULT {JsonSerializer.Serialize(row)}"); Console.Out.Flush();
}
bool Active(Stopwatch watch, double duration) => watch.Elapsed.TotalSeconds < duration;

void RunOmq()
{
    using var context = new Context();
    using var socket = context.CreateSocket(mode == "pushpull" ? (role == "pull" ? SocketType.Pull : SocketType.Push) : (role == "rep" ? SocketType.Rep : SocketType.Req), new Omq.SocketOptions { Linger = 0, ReceiveTimeout = TimeSpan.FromMilliseconds(20), SendTimeout = TimeSpan.FromMilliseconds(1000) });
    if (role is "pull" or "rep") socket.Bind(endpoint); else socket.Connect(endpoint);
    Ready();
    if (mode == "pushpull")
    {
        if (role == "push") { while (true) { try { socket.Send(payload); } catch (OmqAgainException) { } } }
        else
        {
            WaitFirstOmq(socket);
            DrainOmq(socket, warmup);
            var watch = Stopwatch.StartNew(); long count = 0;
            while (Active(watch, seconds)) { try { socket.Receive(); count++; } catch (OmqAgainException) { } }
            Result(count, watch.Elapsed.TotalSeconds);
        }
    }
    else if (role == "req")
    {
        DrainReqOmq(socket, warmup);
        var watch = Stopwatch.StartNew(); long count = 0; var samples = new List<double>();
        while (Active(watch, seconds)) { var one = Stopwatch.StartNew(); socket.Send(payload); socket.Receive(); samples.Add(one.Elapsed.TotalMicroseconds); count++; }
        samples.Sort(); Result(count, watch.Elapsed.TotalSeconds, samples.Count == 0 ? 0 : samples[samples.Count / 2]);
    }
    else
    {
        WaitFirstOmq(socket); socket.Send(payload);
        DrainRepOmq(socket, warmup);
        var watch = Stopwatch.StartNew(); long count = 0;
        while (Active(watch, seconds)) { try { socket.Receive(); socket.Send(payload); count++; } catch (OmqAgainException) { } }
        Result(count, watch.Elapsed.TotalSeconds);
    }
}

void DrainOmq(Omq.Socket socket, double duration)
{
    var watch = Stopwatch.StartNew(); while (Active(watch, duration)) { try { socket.Receive(); } catch (OmqAgainException) { } }
}
void WaitFirstOmq(Omq.Socket socket)
{
    while (true) { try { socket.Receive(); return; } catch (OmqAgainException) { } }
}
void DrainReqOmq(Omq.Socket socket, double duration)
{
    var watch = Stopwatch.StartNew(); while (Active(watch, duration)) { socket.Send(payload); try { socket.Receive(); } catch (OmqAgainException) { } }
}
void DrainRepOmq(Omq.Socket socket, double duration)
{
    var watch = Stopwatch.StartNew(); while (Active(watch, duration)) { try { socket.Receive(); socket.Send(payload); } catch (OmqAgainException) { } }
}

void RunNetMq()
{
    if (mode == "pushpull") RunNetMqPushPull(); else RunNetMqReqRep();
}

void RunOmqAsync()
{
    if (mode == "reqrep" && role == "req") RunOmqAsyncReq();
    else RunOmqAsyncOther().GetAwaiter().GetResult();
}

void RunOmqAsyncReq()
{
    using var context = new Context();
    using var socket = context.CreateSocket(SocketType.Req, new Omq.SocketOptions { Linger = 0 });
    socket.Connect(endpoint); Ready();
    var warm = Stopwatch.StartNew(); while (Active(warm, warmup)) { socket.SendAsync(payload).GetAwaiter().GetResult(); socket.ReceiveAsync().GetAwaiter().GetResult(); }
    var watch = Stopwatch.StartNew(); long count = 0; var samples = new List<double>(); while (Active(watch, seconds)) { var one = Stopwatch.StartNew(); socket.SendAsync(payload).GetAwaiter().GetResult(); socket.ReceiveAsync().GetAwaiter().GetResult(); samples.Add(one.Elapsed.TotalMicroseconds); count++; }
    samples.Sort(); Result(count, watch.Elapsed.TotalSeconds, samples[samples.Count / 2]);
}

async Task RunOmqAsyncOther()
{
    using var context = new Context();
    using var socket = context.CreateSocket(mode == "pushpull" ? (role == "pull" ? SocketType.Pull : SocketType.Push) : (role == "rep" ? SocketType.Rep : SocketType.Req), new Omq.SocketOptions { Linger = 0 });
    if (role is "pull" or "rep") socket.Bind(endpoint); else socket.Connect(endpoint); Ready();
    if (mode == "pushpull" && role == "push") { while (true) await socket.SendAsync(payload); }
    if (mode == "pushpull")
    {
        await socket.ReceiveAsync(); var warm = Stopwatch.StartNew(); while (Active(warm, warmup)) await socket.ReceiveAsync();
        var watch = Stopwatch.StartNew(); long count = 0; while (Active(watch, seconds)) { await socket.ReceiveAsync(); count++; }
        Result(count, watch.Elapsed.TotalSeconds); return;
    }
    await socket.ReceiveAsync(); await socket.SendAsync(payload); var warmRep = Stopwatch.StartNew(); while (Active(warmRep, warmup)) { await socket.ReceiveAsync(); await socket.SendAsync(payload); }
    var repWatch = Stopwatch.StartNew(); long repCount = 0; while (Active(repWatch, seconds)) { await socket.ReceiveAsync(); await socket.SendAsync(payload); repCount++; }
    Result(repCount, repWatch.Elapsed.TotalSeconds);
}

void RunNetMqAsync()
{
    using var runtime = new NetMQRuntime();
    runtime.Run(RunNetMqAsyncCore());
}

async Task RunNetMqAsyncCore()
{
    if (mode == "pushpull")
    {
        using NetMQSocket socket = role == "pull" ? new PullSocket() : new PushSocket(); socket.Options.Linger = TimeSpan.Zero; if (role == "pull") socket.Bind(endpoint); else socket.Connect(endpoint); Ready();
        if (role == "push") { while (true) { await Task.Run(() => socket.SendFrame(payload)); } }
        await socket.ReceiveFrameBytesAsync();
        var warm = Stopwatch.StartNew(); while (Active(warm, warmup)) await socket.ReceiveFrameBytesAsync(); var watch = Stopwatch.StartNew(); long count = 0; while (Active(watch, seconds)) { await socket.ReceiveFrameBytesAsync(); count++; }
        Result(count, watch.Elapsed.TotalSeconds); return;
    }
    if (role == "req")
    {
        using var socket = new RequestSocket(); socket.Options.Linger = TimeSpan.Zero; socket.Connect(endpoint); Ready(); var warm = Stopwatch.StartNew(); while (Active(warm, warmup)) { await Task.Run(() => socket.SendFrame(payload)); await socket.ReceiveFrameBytesAsync(); }
        var watch = Stopwatch.StartNew(); long count = 0; var samples = new List<double>(); while (Active(watch, seconds)) { var one = Stopwatch.StartNew(); await Task.Run(() => socket.SendFrame(payload)); await socket.ReceiveFrameBytesAsync(); samples.Add(one.Elapsed.TotalMicroseconds); count++; }
        samples.Sort(); Result(count, watch.Elapsed.TotalSeconds, samples[samples.Count / 2]); return;
    }
    using var rep = new ResponseSocket(); rep.Options.Linger = TimeSpan.Zero; rep.Bind(endpoint); Ready(); await rep.ReceiveFrameBytesAsync(); await Task.Run(() => rep.SendFrame(payload)); var warmRep = Stopwatch.StartNew(); while (Active(warmRep, warmup)) { await rep.ReceiveFrameBytesAsync(); await Task.Run(() => rep.SendFrame(payload)); }
    var repWatch = Stopwatch.StartNew(); long repCount = 0; while (Active(repWatch, seconds)) { await rep.ReceiveFrameBytesAsync(); await Task.Run(() => rep.SendFrame(payload)); repCount++; }
    Result(repCount, repWatch.Elapsed.TotalSeconds);
}
void RunNetMqPushPull()
{
    NetMQSocket socket = role == "pull" ? new PullSocket() : new PushSocket(); using (socket)
    {
        socket.Options.Linger = TimeSpan.Zero;
        if (role == "pull") socket.Bind(endpoint); else socket.Connect(endpoint); Ready();
        if (role == "push") { while (true) socket.SendFrame(payload); }
        while (!socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(20), out _)) { }
        var warm = Stopwatch.StartNew(); while (Active(warm, warmup)) { socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(20), out _); }
        var watch = Stopwatch.StartNew(); long count = 0; while (Active(watch, seconds)) { if (socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(20), out _)) count++; }
        Result(count, watch.Elapsed.TotalSeconds);
    }
}
void RunNetMqReqRep()
{
    if (role == "req")
    {
        using var socket = new RequestSocket(); socket.Options.Linger = TimeSpan.Zero; socket.Connect(endpoint); Ready();
        var warm = Stopwatch.StartNew(); while (Active(warm, warmup)) { socket.SendFrame(payload); socket.ReceiveFrameBytes(); }
        var watch = Stopwatch.StartNew(); long count = 0; var samples = new List<double>();
        while (Active(watch, seconds)) { var one = Stopwatch.StartNew(); socket.SendFrame(payload); socket.ReceiveFrameBytes(); samples.Add(one.Elapsed.TotalMicroseconds); count++; }
        samples.Sort(); Result(count, watch.Elapsed.TotalSeconds, samples.Count == 0 ? 0 : samples[samples.Count / 2]); return;
    }
    using var rep = new ResponseSocket(); rep.Options.Linger = TimeSpan.Zero; rep.Bind(endpoint); Ready();
    while (!rep.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(20), out _)) { }
    rep.SendFrame(payload);
    var warmRep = Stopwatch.StartNew(); while (Active(warmRep, warmup)) { if (rep.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(20), out _)) rep.SendFrame(payload); }
    var watchRep = Stopwatch.StartNew(); long countRep = 0; while (Active(watchRep, seconds)) { if (rep.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(20), out _)) { rep.SendFrame(payload); countRep++; } }
    Result(countRep, watchRep.Elapsed.TotalSeconds);
}
