namespace Omq;

/// Blocking built-in proxy/device helpers. Run them on a dedicated thread.
public static class Proxy
{
    /// Runs a blocking proxy between frontend and backend sockets.
    public static void Run(Socket frontend, Socket backend, Socket? capture = null)
    {
        using var leases = new Socket.NativeLeaseSet([frontend, backend, capture]);
        Errors.Check("proxy", Native.zmq_proxy(leases[frontend], leases[backend], leases.Get(capture)));
    }

    /// Runs a blocking steerable proxy.
    public static void RunSteerable(Socket frontend, Socket backend, Socket? capture, Socket control)
    {
        using var leases = new Socket.NativeLeaseSet([frontend, backend, capture, control]);
        Errors.Check("proxy_steerable", Native.zmq_proxy_steerable(leases[frontend], leases[backend], leases.Get(capture), leases[control]));
    }

    /// Runs a legacy native device by numeric device type.
    public static void Device(int deviceType, Socket frontend, Socket backend)
    {
        using var leases = new Socket.NativeLeaseSet([frontend, backend]);
        Errors.Check("device", Native.zmq_device(deviceType, leases[frontend], leases[backend]));
    }
}
