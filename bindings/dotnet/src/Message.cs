namespace Omq;

/// Managed, owning representation of one or more OMQ message frames.
public sealed class Message
{
    private readonly byte[][] parts;
    /// Gets the message frames. Frame bytes are owned by this message.
    public IReadOnlyList<byte[]> Parts => parts;
    /// Gets the number of frames.
    public int PartCount => parts.Length;
    /// Gets a frame by index.
    public byte[] this[int index] => parts[index];
    /// Gets the only frame; throws for multipart messages.
    public byte[] Data => parts.Length == 1 ? parts[0] : throw new InvalidOperationException("message is multipart");
    /// Gets whether this message contains more than one frame.
    public bool IsMultipart => parts.Length > 1;
    /// Gets or sets the routing ID carried by the first frame.
    public uint RoutingId { get; set; }
    internal Message(byte[][] parts) => this.parts = parts;
    /// Copies one frame into a new message.
    public Message(byte[] data) : this(new[] { data.ToArray() }) { }
    /// Copies all supplied frames into a new message.
    public Message(IEnumerable<ReadOnlyMemory<byte>> parts) : this(parts.Select(x => x.ToArray()).ToArray()) { }
    /// Creates a UTF-8 single-frame message.
    public static Message Text(string text) => new(System.Text.Encoding.UTF8.GetBytes(text));
    /// Deep-copies this message, including frame bytes and routing ID.
    public Message Clone() => new(parts.Select(part => part.ToArray()).ToArray()) { RoutingId = RoutingId };
    public override string ToString() => IsMultipart ? $"multipart({parts.Length})" : System.Text.Encoding.UTF8.GetString(Data);
}
