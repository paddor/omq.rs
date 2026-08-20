using System.Runtime.InteropServices;

namespace Omq;

public readonly record struct CurveKeyPair(string PublicKey, string SecretKey);

public static class Curve
{
    public static CurveKeyPair GenerateKeyPair()
    {
        IntPtr publicKey = Marshal.AllocHGlobal(41), secretKey = Marshal.AllocHGlobal(41);
        try
        {
            Errors.Check("curve_keypair", Native.zmq_curve_keypair(publicKey, secretKey));
            return new CurveKeyPair(Marshal.PtrToStringAnsi(publicKey)!, Marshal.PtrToStringAnsi(secretKey)!);
        }
        finally { Marshal.FreeHGlobal(publicKey); Marshal.FreeHGlobal(secretKey); }
    }

    public static string PublicKey(string secretKey)
    {
        IntPtr publicKey = Marshal.AllocHGlobal(41), secret = Marshal.StringToCoTaskMemUTF8(secretKey);
        try { Errors.Check("curve_public", Native.zmq_curve_public(publicKey, secret)); return Marshal.PtrToStringAnsi(publicKey)!; }
        finally { Marshal.FreeHGlobal(publicKey); Marshal.FreeCoTaskMem(secret); }
    }
}
