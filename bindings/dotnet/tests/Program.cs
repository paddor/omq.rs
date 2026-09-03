using Omq;

static void Check(bool condition, string message)
{
    if (!condition) throw new Exception(message);
}
static Message ReceiveEventually(Socket socket)
{
    DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
    while (true)
    {
        try { return socket.Receive(dontWait: true); }
        catch (OmqAgainException) when (DateTime.UtcNow < deadline) { Thread.Sleep(10); }
    }
}

using var context = new Context();
Check(!context.Closed, "context unexpectedly closed");
var key = context.ShareKey();
using var peer = Context.FromShareKey(key.High, key.Low);
using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0, ReceiveTimeout = TimeSpan.FromSeconds(2) });
using var push = peer.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0, SendTimeout = TimeSpan.FromSeconds(2) });

pull.Bind("inproc://dotnet-smoke");
push.Connect("inproc://dotnet-smoke");
push.Send(Message.Text("hello"));
Check(pull.Receive().ToString() == "hello", "single-part message mismatch");
var cloned = Message.Text("clone"); cloned.RoutingId = 7;
Check(cloned.Clone().ToString() == "clone" && cloned.Clone().RoutingId == 7, "message clone mismatch");

using var req = context.CreateSocket(SocketType.Req, new SocketOptions { Linger = 0 });
using var rep = context.CreateSocket(SocketType.Rep, new SocketOptions { Linger = 0 });
rep.Bind("inproc://dotnet-reqrep");
req.Connect("inproc://dotnet-reqrep");
req.Send([9, 8, 7]);
Check(rep.Receive().Data.SequenceEqual(new byte[] { 9, 8, 7 }), "REQ/REP request mismatch");
rep.Send([6, 5]);
Check(req.Receive().Data.SequenceEqual(new byte[] { 6, 5 }), "REQ/REP reply mismatch");
req.SendJson(new { value = 42 });
var json = rep.ReceiveJson<Dictionary<string, int>>();
Check(json is not null && json["value"] == 42, "JSON message mismatch");
rep.SendString("string");
Check(req.ReceiveString() == "string", "string helper mismatch");

using var options = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0, SendHwm = 64, ReceiveHwm = 32, HeartbeatInterval = 1000 });
Check(options.GetInt32(SocketOption.SendHwm) == 64, "SNDHWM option mismatch");
Check(options.GetInt32(SocketOption.ReceiveHwm) == 32, "RCVHWM option mismatch");
Check(options.GetInt32(SocketOption.HeartbeatInterval) == 1000, "heartbeat option mismatch");
options.SetOption(SocketOption.RoutingId, [1, 2, 3]);
Check(options.GetBytes(SocketOption.RoutingId).SequenceEqual(new byte[] { 1, 2, 3 }), "routing id mismatch");
Check(!pull.TryReceive(out _), "TryReceive should report empty socket");
using var dynamicWs = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0 });
string dynamicWsEndpoint = dynamicWs.Bind("ws://127.0.0.1:0/dotnet-dynamic");
Check(new Uri(dynamicWsEndpoint).Port != 0, "dynamic WebSocket bind returned port zero");

using var pub = context.CreateSocket(SocketType.Pub, new SocketOptions { Linger = 0 });
using var sub = context.CreateSocket(SocketType.Sub, new SocketOptions { Linger = 0, ReceiveTimeout = TimeSpan.FromSeconds(1) });
pub.Bind("inproc://dotnet-pubsub");
sub.Connect("inproc://dotnet-pubsub");
sub.Subscribe("topic");
for (int i = 0; i < 10 && !sub.TryReceive(out _); i++) pub.SendText("topic:hello");
pub.SendText("topic:hello");
Check(sub.ReceiveText() == "topic:hello", "PUB/SUB message mismatch");

var multipart = new Message(new[]
{
    new ReadOnlyMemory<byte>([1, 2, 3]),
    new ReadOnlyMemory<byte>([4, 5])
});
push.Send(multipart);
Message received = pull.Receive();
Check(received.Parts.Count == 2, "multipart part count mismatch");
Check(received.Parts[0].SequenceEqual(new byte[] { 1, 2, 3 }), "multipart first part mismatch");
Check(received.Parts[1].SequenceEqual(new byte[] { 4, 5 }), "multipart second part mismatch");

byte[] largeFrame = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();
push.Send(largeFrame);
byte[] shortBuffer = new byte[10];
byte[] truncated = pull.ReceiveInto(shortBuffer);
Check(truncated.Length == shortBuffer.Length, "ReceiveInto returned the untruncated frame length");
Check(truncated.SequenceEqual(largeFrame[..shortBuffer.Length]), "ReceiveInto truncated data mismatch");

await push.SendAsync(Message.Text("async"));
Check((await pull.ReceiveAsync()).ToString() == "async", "async message mismatch");
await push.SendAsync(new Message([new ReadOnlyMemory<byte>([1, 2]), new ReadOnlyMemory<byte>([3, 4])]));
var asyncMultipart = await pull.ReceiveAsync();
Check(asyncMultipart.IsMultipart && asyncMultipart.Parts.Count == 2 && asyncMultipart.Parts[1].SequenceEqual(new byte[] { 3, 4 }), "async multipart mismatch");
using (var cancelled = new CancellationTokenSource(TimeSpan.FromMilliseconds(20)))
{
    bool observed = false;
    try { await pull.ReceiveAsync(cancelled.Token); }
    catch (OperationCanceledException) { observed = true; }
    Check(observed, "async receive cancellation mismatch");
}
using var poller = new Poller(); poller.Add(pull);
push.Send(Message.Text("poll"));
Check(poller.Wait(TimeSpan.FromSeconds(1)).Count != 0, "poller did not report readable socket");
Check(pull.Receive().ToString() == "poll", "poller message mismatch");
Check(pull.Poll(TimeSpan.Zero).Count == 0, "socket poll should be empty");
using (var zeroTimeoutGuard = new CancellationTokenSource(TimeSpan.FromMilliseconds(200)))
{
    Check((await poller.WaitAsync(TimeSpan.Zero, zeroTimeoutGuard.Token)).Count == 0,
        "async zero-timeout poll should be empty");
}
var keys = Curve.GenerateKeyPair();
Check(keys.PublicKey.Length == 40 && keys.SecretKey.Length == 40, "CURVE keypair mismatch");
Check(Curve.PublicKey(keys.SecretKey) == keys.PublicKey, "CURVE public key mismatch");
var serverKeys = Curve.GenerateKeyPair();
var otherKeys = Curve.GenerateKeyPair();
using (var invalidCurve = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0 }))
{
    invalidCurve.ConfigureCurveClient(keys.PublicKey, otherKeys.SecretKey, serverKeys.PublicKey);
    bool rejected = false;
    try { invalidCurve.Connect("tcp://127.0.0.1:1"); }
    catch (OmqException error) when (error.Errno == 22) { rejected = true; }
    Check(rejected, "mismatched CURVE keypair should return EINVAL");
}
using var curveRep = context.CreateSocket(SocketType.Rep, new SocketOptions { Linger = 0 });
using var curveReq = context.CreateSocket(SocketType.Req, new SocketOptions { Linger = 0 });
curveRep.ConfigureCurveServer(serverKeys.PublicKey, serverKeys.SecretKey);
curveReq.ConfigureCurveClient(keys.PublicKey, keys.SecretKey, serverKeys.PublicKey);
string curveEndpoint = curveRep.Bind("tcp://127.0.0.1:0");
curveReq.Connect(curveEndpoint);
curveReq.SendText("curve");
Check(ReceiveEventually(curveRep).ToString() == "curve", "CURVE request mismatch");
curveRep.SendText("ok");
Check(ReceiveEventually(curveReq).ToString() == "ok", "CURVE reply mismatch");
using var plainRep = context.CreateSocket(SocketType.Rep, new SocketOptions { Linger = 0 });
using var plainReq = context.CreateSocket(SocketType.Req, new SocketOptions { Linger = 0 });
plainRep.ConfigurePlainServer("user", "pass");
plainReq.ConfigurePlainClient("user", "pass");
string plainEndpoint = plainRep.Bind("tcp://127.0.0.1:0");
plainReq.Connect(plainEndpoint);
plainReq.SendText("plain");
Check(ReceiveEventually(plainRep).ToString() == "plain", "PLAIN request mismatch");
plainRep.SendText("ok");
Check(ReceiveEventually(plainReq).ToString() == "ok", "PLAIN reply mismatch");
using var monitor = pull.Monitor();
using (var idleSocket = context.CreateSocket(SocketType.Pair, new SocketOptions { Linger = 0 }))
using (var idleMonitor = idleSocket.Monitor())
{
    using var cancelled = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
    bool observed = false;
    try { await idleMonitor.ReceiveAsync(cancelled.Token); }
    catch (OperationCanceledException) { observed = true; }
    Check(observed, "monitor receive cancellation mismatch");
}

foreach (SocketType type in Enum.GetValues<SocketType>())
{
    if (type == SocketType.Dgram) continue; // omq-libzmq has no DGRAM mapping yet.
    using Socket socket = context.CreateSocket(type, new SocketOptions { Linger = 0 });
    Check(socket.Type == type, $"socket type mismatch: {type}");
}

using (var doomed = new Context())
using (var waiting = doomed.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0 }))
{
    Task<Message> pending = waiting.ReceiveAsync();
    doomed.Shutdown();
    bool ended = false;
    try { await pending.WaitAsync(TimeSpan.FromSeconds(1)); }
    catch (OmqException) { ended = true; }
    catch (ObjectDisposedException) { ended = true; }
    Check(ended, "context shutdown did not wake receive");
}

Console.WriteLine("OMQ.Net smoke: PASS");
