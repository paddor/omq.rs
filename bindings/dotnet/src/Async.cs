namespace Omq;

using System.Buffers.Binary;
using System.Runtime.InteropServices;

/// Cancellation-aware asynchronous socket operations.
public static class SocketAsyncExtensions
{
    /// Sends one frame, waiting for writability when the socket HWM is full.
    public static async Task SendAsync(this Socket socket, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var poller = new Poller(); poller.Add(socket, PollEvents.Writable);
        while (!socket.TrySend(data.Span)) { cancellationToken.ThrowIfCancellationRequested(); await poller.WaitAsync(TimeSpan.FromMilliseconds(100), cancellationToken).ConfigureAwait(false); }
    }

    /// Sends all frames in a message and completes when the native async send finishes.
    public static async Task SendAsync(this Socket socket, Message message, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        byte[] encoded = Encode(message);
        var state = new AsyncState();
        GCHandle root = GCHandle.Alloc(state);
        IntPtr task;
        unsafe
        {
            fixed (byte* p = encoded)
                task = Native.omq_socket_send_async(socket.NativeHandle, (IntPtr)p, (nuint)encoded.Length, Complete, GCHandle.ToIntPtr(root));
        }
        if (task == IntPtr.Zero)
        {
            root.Free();
            throw new OmqException("send_async", Native.zmq_errno(), "native async send creation failed");
        }
        state.Task = task;
        using var registration = cancellationToken.Register(static state => Native.omq_async_task_cancel((IntPtr)state!), task);
        try
        {
            await state.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Keep the native handle and GC root alive until the cancelled
            // Tokio task has observed cancellation and invoked the callback.
            try { await state.Completion.Task.ConfigureAwait(false); } catch { }
            throw;
        }
        finally
        {
            registration.Dispose();
            Native.omq_async_task_free(task);
            root.Free();
        }
    }

    /// Receives the next complete message, waiting asynchronously until available.
    public static Task<Message> ReceiveAsync(this Socket socket, CancellationToken cancellationToken = default)
    {
        return ReceiveAsyncCore(socket, cancellationToken);
    }

    private static async Task<Message> ReceiveAsyncCore(Socket socket, CancellationToken cancellationToken)
    {
        var poller = new Poller(); poller.Add(socket, PollEvents.Readable);
        Message? message;
        while (!socket.TryReceive(out message))
        {
            cancellationToken.ThrowIfCancellationRequested();
            await poller.WaitAsync(TimeSpan.FromMilliseconds(100), cancellationToken).ConfigureAwait(false);
        }
        return message!;
    }

    private static byte[] Encode(Message message)
    {
        int header = checked(8 + message.Parts.Count * 8);
        int total = header + message.Parts.Sum(part => part.Length);
        byte[] encoded = new byte[total];
        BinaryPrimitives.WriteUInt64LittleEndian(encoded, (ulong)message.Parts.Count);
        int offset = header;
        for (int i = 0; i < message.Parts.Count; i++)
        {
            byte[] part = message.Parts[i];
            BinaryPrimitives.WriteUInt64LittleEndian(encoded.AsSpan(8 + i * 8), (ulong)part.Length);
            part.CopyTo(encoded, offset); offset += part.Length;
        }
        return encoded;
    }

    private sealed class AsyncState
    {
        internal readonly TaskCompletionSource<object?> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal IntPtr Task;
    }

    private static readonly Native.AsyncCallback Complete = CompleteNative;
    private static void CompleteNative(IntPtr userdata, int status)
    {
        var root = GCHandle.FromIntPtr(userdata);
        var state = (AsyncState)root.Target!;
        while (state.Task == IntPtr.Zero) Thread.Yield();
        if (status == 0) state.Completion.TrySetResult(null);
        else if (status == 125) state.Completion.TrySetCanceled();
        else state.Completion.TrySetException(new OmqException("send_async", status, "native async send failed"));
    }
}
