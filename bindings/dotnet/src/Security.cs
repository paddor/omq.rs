using System.Runtime.InteropServices;

namespace Omq;

/// <summary>One exact username/password pair accepted by a PLAIN server.</summary>
/// <param name="Username">PLAIN username containing at most 255 ASCII VCHAR bytes.</param>
/// <param name="Password">PLAIN password containing at most 255 ASCII VCHAR bytes.</param>
public readonly record struct PlainCredential(string Username, string Password);

/// A Z85-encoded CURVE public/secret key pair.
public readonly record struct CurveKeyPair(string PublicKey, string SecretKey);

/// CURVE key generation helpers.
public static class Curve
{
    /// Generates a new CURVE key pair.
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

    /// Derives the Z85 public key corresponding to a secret key.
    public static string PublicKey(string secretKey)
    {
        IntPtr publicKey = Marshal.AllocHGlobal(41), secret = Marshal.StringToCoTaskMemUTF8(secretKey);
        try { Errors.Check("curve_public", Native.zmq_curve_public(publicKey, secret)); return Marshal.PtrToStringAnsi(publicKey)!; }
        finally { Marshal.FreeHGlobal(publicKey); Marshal.FreeCoTaskMem(secret); }
    }
}
