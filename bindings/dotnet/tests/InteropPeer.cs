using Omq;

if (args.Length < 4) throw new ArgumentException("role endpoint security server_key [client_public client_secret]");
string role = args[0], endpoint = args[1], security = args[2];
var options = new SocketOptions { Linger = 0, ReceiveTimeout = TimeSpan.FromSeconds(5), SendTimeout = TimeSpan.FromSeconds(5) };
using var context = new Context();
using var socket = context.CreateSocket(role == "rep" ? SocketType.Rep : SocketType.Req, options);
if (security == "curve")
{
    if (role == "rep") socket.ConfigureCurveServer(args[3], args[4]);
    else socket.ConfigureCurveClient(args[4], args[5], args[3]);
}
else if (security == "plain")
{
    if (role == "rep") socket.ConfigurePlainServer();
    else socket.ConfigurePlainClient("interop", "secret");
}
if (role == "rep") socket.Bind(endpoint); else socket.Connect(endpoint);
if (role == "req")
{
    socket.SendMultipart(["interop"u8.ToArray(), "hello"u8.ToArray()]);
    var reply = socket.Receive();
    if (reply.Parts.Count != 2 || !reply.Parts[0].SequenceEqual("interop"u8) || !reply.Parts[1].SequenceEqual("world"u8)) throw new Exception("interop reply mismatch");
}
else
{
    var request = socket.Receive();
    if (request.Parts.Count != 2 || !request.Parts[0].SequenceEqual("interop"u8) || !request.Parts[1].SequenceEqual("hello"u8)) throw new Exception("interop request mismatch");
    socket.SendMultipart(["interop"u8.ToArray(), "world"u8.ToArray()]);
}
Console.WriteLine("interop PASS");
