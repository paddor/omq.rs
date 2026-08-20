using System.Runtime.InteropServices;

namespace Omq;

public class OmqException : Exception
{
    public int Errno { get; }
    internal OmqException(string operation, int errno, string message) : base($"{operation}: {message} (errno {errno})") => Errno = errno;
}

public sealed class OmqAgainException : OmqException { internal OmqAgainException(string op, int e, string m) : base(op, e, m) { } }
public sealed class OmqClosedException : ObjectDisposedException { internal OmqClosedException() : base("OMQ handle") { } }

internal static class Errors
{
    internal static void Check(string operation, int result)
    {
        if (result >= 0) return;
        int errno = Native.zmq_errno();
        string message = Marshal.PtrToStringAnsi(Native.zmq_strerror(errno)) ?? "native error";
        if (errno is 11 or 156384715) throw new OmqAgainException(operation, errno, message);
        throw new OmqException(operation, errno, message);
    }
}
