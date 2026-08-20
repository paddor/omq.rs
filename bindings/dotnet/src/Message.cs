namespace Omq;

public sealed class Message
{
    private readonly byte[][] parts;
    public IReadOnlyList<byte[]> Parts => parts;
    public int PartCount => parts.Length;
    public byte[] this[int index] => parts[index];
    public byte[] Data => parts.Length == 1 ? parts[0] : throw new InvalidOperationException("message is multipart");
    public bool IsMultipart => parts.Length > 1;
    public uint RoutingId { get; set; }
    internal Message(byte[][] parts) => this.parts = parts;
    public Message(byte[] data) : this(new[] { data.ToArray() }) { }
    public Message(IEnumerable<ReadOnlyMemory<byte>> parts) : this(parts.Select(x => x.ToArray()).ToArray()) { }
    public static Message Text(string text) => new(System.Text.Encoding.UTF8.GetBytes(text));
    public Message Clone() => new(parts.Select(part => part.ToArray()).ToArray()) { RoutingId = RoutingId };
    public override string ToString() => IsMultipart ? $"multipart({parts.Length})" : System.Text.Encoding.UTF8.GetString(Data);
}
