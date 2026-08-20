using Omq;

using var context = new Context();
using var pull = context.CreateSocket(SocketType.Pull, new SocketOptions { Linger = 0 });
using var push = context.CreateSocket(SocketType.Push, new SocketOptions { Linger = 0 });

var endpoint = pull.Bind("inproc://package-smoke");
push.Connect(endpoint);
push.Send(Message.Text("package"));
if (pull.Receive().ToString() != "package")
    throw new InvalidOperationException("package smoke exchange failed");
