using System.Runtime.InteropServices;

namespace Omq;

internal static partial class Native
{
    private const string Library = "omq_zmq";
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AsyncCallback(IntPtr userdata, int status);

    [StructLayout(LayoutKind.Sequential, Size = 64)]
    internal struct Message { }

    [StructLayout(LayoutKind.Sequential)]
    internal struct PollItem
    {
        internal IntPtr Socket;
        internal int FileDescriptor;
        internal short Events;
        internal short Revents;
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr zmq_ctx_new();
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_ctx_set(IntPtr ctx, int option, int value);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_ctx_get(IntPtr ctx, int option);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_ctx_term(IntPtr ctx);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_ctx_shutdown(IntPtr ctx);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr zmq_socket(IntPtr ctx, int type);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int omq_socket_allow_thread_migration(IntPtr socket);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int omq_socket_set_plain_server_credentials(IntPtr socket, IntPtr username, nuint usernameLength, IntPtr password, nuint passwordLength);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_close(IntPtr socket);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_setsockopt(IntPtr socket, int option, IntPtr value, nuint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_getsockopt(IntPtr socket, int option, IntPtr value, ref nuint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zmq_bind")] internal static extern int Bind(IntPtr socket, IntPtr endpoint);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zmq_connect")] internal static extern int Connect(IntPtr socket, IntPtr endpoint);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zmq_unbind")] internal static extern int Unbind(IntPtr socket, IntPtr endpoint);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zmq_disconnect")] internal static extern int Disconnect(IntPtr socket, IntPtr endpoint);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_send(IntPtr socket, IntPtr buffer, nuint length, int flags);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_recv(IntPtr socket, IntPtr buffer, nuint length, int flags);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_msg_init_size(ref Message message, nuint size);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_msg_init(ref Message message);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_msg_close(ref Message message);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_msg_send(ref Message message, IntPtr socket, int flags);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_msg_recv(ref Message message, IntPtr socket, int flags);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr zmq_msg_data(ref Message message);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern nuint zmq_msg_size(ref Message message);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_msg_more(ref Message message);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern uint zmq_msg_routing_id(ref Message message);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_msg_set_routing_id(ref Message message, uint routingId);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_errno();
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr zmq_strerror(int error);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_poll(IntPtr items, int count, int timeout);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int omq_ctx_share_key(IntPtr ctx, out ulong high, out ulong low);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr omq_ctx_from_share_key(ulong high, ulong low);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_curve_keypair(IntPtr publicKey, IntPtr secretKey);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_curve_public(IntPtr publicKey, IntPtr secretKey);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_socket_monitor(IntPtr socket, IntPtr endpoint, int events);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_proxy(IntPtr frontend, IntPtr backend, IntPtr capture);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_proxy_steerable(IntPtr frontend, IntPtr backend, IntPtr capture, IntPtr control);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int zmq_device(int device, IntPtr frontend, IntPtr backend);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr omq_socket_send_async(IntPtr socket, IntPtr encoded, nuint encodedLength, AsyncCallback callback, IntPtr userdata);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void omq_async_task_cancel(IntPtr task);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void omq_async_task_free(IntPtr task);
}
