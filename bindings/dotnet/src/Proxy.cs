namespace Omq;

/// Blocking built-in proxy/device helpers. Run them on a dedicated thread.
public static class Proxy
{
    /// Runs a blocking proxy between frontend and backend sockets.
    public static void Run(Socket frontend, Socket backend, Socket? capture = null) =>
        Errors.Check("proxy", Native.zmq_proxy(frontend.NativeHandle, backend.NativeHandle, capture?.NativeHandle ?? IntPtr.Zero));

    /// Runs a blocking steerable proxy.
    public static void RunSteerable(Socket frontend, Socket backend, Socket? capture, Socket control) =>
        Errors.Check("proxy_steerable", Native.zmq_proxy_steerable(frontend.NativeHandle, backend.NativeHandle, capture?.NativeHandle ?? IntPtr.Zero, control.NativeHandle));

    /// Runs a legacy native device by numeric device type.
    public static void Device(int deviceType, Socket frontend, Socket backend) =>
        Errors.Check("device", Native.zmq_device(deviceType, frontend.NativeHandle, backend.NativeHandle));
}
