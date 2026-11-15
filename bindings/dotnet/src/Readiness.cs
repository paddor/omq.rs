using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Omq;

/// <summary>
/// Wakes async waiters from the native ready callback
/// (<c>omq_socket_set_ready_callback</c>). The callback runs on an OMQ I/O
/// thread and only completes a task whose continuations run on the thread
/// pool, so no managed caller code runs on that thread.
/// </summary>
internal sealed class Readiness
{
    private readonly object sync = new();
    private TaskCompletionSource? waiter;
    private Exception? closed;
    private GCHandle self;

    /// Roots this object for native code and returns the callback userdata.
    internal IntPtr Root()
    {
        self = GCHandle.Alloc(this);
        return GCHandle.ToIntPtr(self);
    }

    /// Unroots after native close or a failed registration; no callback runs
    /// or follows by then.
    internal void Unroot()
    {
        if (self.IsAllocated) self.Free();
    }

    /// The task for the next readiness change. Take it before arming the
    /// callback so a callback in between completes it.
    internal Task Next()
    {
        lock (sync)
        {
            if (closed is not null) return Task.FromException(closed);
            waiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return waiter.Task;
        }
    }

    /// Completes the current waiters; later ones get a new task.
    internal void Signal()
    {
        TaskCompletionSource? current;
        lock (sync) { current = waiter; waiter = null; }
        current?.TrySetResult();
    }

    /// Fails current and later waiters once the socket is closed.
    internal void Close()
    {
        TaskCompletionSource? current;
        Exception error;
        lock (sync) { error = closed ??= new OmqClosedException(); current = waiter; waiter = null; }
        current?.TrySetException(error);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnReady(IntPtr userdata)
    {
        if (GCHandle.FromIntPtr(userdata).Target is Readiness readiness) readiness.Signal();
    }
}
