namespace Omq;

public static class ContextOption
{
    public const int IoThreads = 1, MaxSockets = 2, SocketLimit = 3, ThreadPriority = 3;
    public const int ThreadSchedulerPolicy = 4, MaxMessageSize = 5, MessageSize = 6;
    public const int ThreadAffinityCpuAdd = 7, ThreadAffinityCpuRemove = 8, ThreadNamePrefix = 9, ZeroCopyReceive = 10;
    public const int Blocky = 70;
}

public static class SocketOption
{
    public const int Affinity = 4, RoutingId = 5, Identity = RoutingId, Subscribe = 6, Unsubscribe = 7;
    public const int Rate = 8, RecoveryInterval = 9, SendBuffer = 11, ReceiveBuffer = 12, ReceiveMore = 13, FileDescriptor = 14;
    public const int Events = 15, Type = 16, Linger = 17, ReconnectInterval = 18;
    public const int Backlog = 19, ReconnectIntervalMax = 21, MaxMessageSize = 22, MulticastHops = 25;
    public const int SendHwm = 23, ReceiveHwm = 24, ReceiveTimeout = 27, SendTimeout = 28;
    public const int LastEndpoint = 32, RouterMandatory = 33, TcpKeepalive = 34;
    public const int TcpKeepaliveCount = 35, TcpKeepaliveIdle = 36, TcpKeepaliveInterval = 37;
    public const int TcpAcceptFilter = 38, Immediate = 39, XPubVerbose = 40, RouterRaw = 41, Ipv6 = 42, Mechanism = 43;
    public const int PlainServer = 44, PlainUsername = 45, PlainPassword = 46;
    public const int CurveServer = 47, CurvePublicKey = 48, CurveSecretKey = 49;
    public const int CurveServerKey = 50, ProbeRouter = 51, ReqCorrelate = 52, ReqRelaxed = 53;
    public const int Conflate = 54, ZapDomain = 55, RouterHandover = 56, Tos = 57, IpcFilterPid = 58, IpcFilterUid = 59, IpcFilterGid = 60, ConnectRoutingId = 61;
    public const int GssapiServer = 62, GssapiPrincipal = 63, GssapiServicePrincipal = 64, GssapiPlaintext = 65;
    public const int HandshakeInterval = 66, SocksProxy = 68, XPubNoDrop = 69, Blocky = 70, XPubManual = 71;
    public const int XPubWelcomeMessage = 72, StreamNotify = 73, InvertMatching = 74;
    public const int HeartbeatInterval = 75, HeartbeatTtl = 76;
    public const int HeartbeatTimeout = 77, XPubVerboser = 78, ConnectTimeout = 79, TcpMaxRt = 80, ThreadSafe = 81;
    public const int MulticastMaxTpdu = 84, VmciBufferSize = 85, VmciBufferMinSize = 86, VmciBufferMaxSize = 87, VmciConnectTimeout = 88, UseFd = 89;
    public const int GssapiPrincipalNameType = 90, GssapiServicePrincipalNameType = 91, BindToDevice = 92, ZapEnforceDomain = 93, LoopbackFastPath = 94;
    public const int Metadata = 95, MulticastLoop = 96, RouterNotify = 97, XPubManualLastValue = 98;
    public const int SocksUsername = 99, SocksPassword = 100, InBatchSize = 101, OutBatchSize = 102;
    public const int WssKeyPem = 103, WssCertPem = 104, WssTrustPem = 105, WssHostname = 106, WssTrustSystem = 107;
    public const int OnlyFirstSubscribe = 108, ReconnectStop = 109, HelloMessage = 110, DisconnectMessage = 111;
    public const int Priority = 112, BusyPoll = 113, HiccupMessage = 114, XSubVerboseUnsubscribe = 115, TopicsCount = 116;
    public const int NormMode = 117, NormUnicastNack = 118, NormBufferSize = 119, NormSegmentSize = 120;
    public const int NormBlockSize = 121, NormNumParity = 122, NormNumAutoParity = 123, NormPush = 124, ArenaThreshold = 10001;
}

public readonly record struct SocketOptions
{
    public long? Affinity { get; init; }
    public int? SendHwm { get; init; }
    public int? ReceiveHwm { get; init; }
    public int? SendBuffer { get; init; }
    public int? ReceiveBuffer { get; init; }
    public int? Linger { get; init; }
    public TimeSpan? SendTimeout { get; init; }
    public TimeSpan? ReceiveTimeout { get; init; }
    public int? Rate { get; init; }
    public int? RecoveryInterval { get; init; }
    public int? Backlog { get; init; }
    public int? MulticastHops { get; init; }
    public byte[]? Identity { get; init; }
    public int? ReconnectInterval { get; init; }
    public int? ReconnectIntervalMax { get; init; }
    public long? MaxMessageSize { get; init; }
    public bool? RouterMandatory { get; init; }
    public bool? Immediate { get; init; }
    public bool? Ipv6 { get; init; }
    public bool? Conflate { get; init; }
    public bool? RouterRaw { get; init; }
    public bool? ProbeRouter { get; init; }
    public bool? ReqCorrelate { get; init; }
    public bool? ReqRelaxed { get; init; }
    public bool? RouterHandover { get; init; }
    public bool? XPubVerbose { get; init; }
    public bool? XPubNoDrop { get; init; }
    public bool? XPubManual { get; init; }
    public bool? XPubVerboser { get; init; }
    public bool? InvertMatching { get; init; }
    public bool? StreamNotify { get; init; }
    public bool? OnlyFirstSubscribe { get; init; }
    public bool? MulticastLoop { get; init; }
    public bool? ZapEnforceDomain { get; init; }
    public int? ConnectTimeout { get; init; }
    public int? TcpMaxRt { get; init; }
    public string? ZapDomain { get; init; }
    public string? ConnectRoutingId { get; init; }
    public string? GssapiPrincipal { get; init; }
    public string? GssapiServicePrincipal { get; init; }
    public string? SocksProxy { get; init; }
    public string? SocksUsername { get; init; }
    public string? SocksPassword { get; init; }
    public string? WssKeyPem { get; init; }
    public string? WssCertPem { get; init; }
    public string? WssTrustPem { get; init; }
    public string? WssHostname { get; init; }
    public bool? WssTrustSystem { get; init; }
    public int? HeartbeatInterval { get; init; }
    public int? HeartbeatTtl { get; init; }
    public int? HeartbeatTimeout { get; init; }
    public int? TcpKeepalive { get; init; }
    public int? TcpKeepaliveCount { get; init; }
    public int? TcpKeepaliveIdle { get; init; }
    public int? TcpKeepaliveInterval { get; init; }

    internal void Apply(Socket socket)
    {
        if (Affinity is { } affinity) socket.SetOption(SocketOption.Affinity, affinity);
        if (SendHwm is { } snd) socket.SetOption(SocketOption.SendHwm, snd);
        if (ReceiveHwm is { } rcv) socket.SetOption(SocketOption.ReceiveHwm, rcv);
        if (Linger is { } linger) socket.SetOption(SocketOption.Linger, linger);
        if (SendTimeout is { } st) socket.SetOption(SocketOption.SendTimeout, ToMilliseconds(st));
        if (ReceiveTimeout is { } rt) socket.SetOption(SocketOption.ReceiveTimeout, ToMilliseconds(rt));
        if (SendBuffer is { } sndBuf) socket.SetOption(SocketOption.SendBuffer, sndBuf);
        if (ReceiveBuffer is { } rcvBuf) socket.SetOption(SocketOption.ReceiveBuffer, rcvBuf);
        if (Rate is { } rate) socket.SetOption(SocketOption.Rate, rate);
        if (RecoveryInterval is { } recovery) socket.SetOption(SocketOption.RecoveryInterval, recovery);
        if (Backlog is { } backlog) socket.SetOption(SocketOption.Backlog, backlog);
        if (MulticastHops is { } hops) socket.SetOption(SocketOption.MulticastHops, hops);
        if (Identity is { } identity) socket.SetOption(SocketOption.RoutingId, identity);
        if (ReconnectInterval is { } reconnect) socket.SetOption(SocketOption.ReconnectInterval, reconnect);
        if (ReconnectIntervalMax is { } reconnectMax) socket.SetOption(SocketOption.ReconnectIntervalMax, reconnectMax);
        if (MaxMessageSize is { } maxMessage) socket.SetOption(SocketOption.MaxMessageSize, maxMessage);
        if (RouterMandatory is { } mandatory) socket.SetOption(SocketOption.RouterMandatory, mandatory ? 1 : 0);
        if (Immediate is { } immediate) socket.SetOption(SocketOption.Immediate, immediate ? 1 : 0);
        if (Ipv6 is { } ipv6) socket.SetOption(SocketOption.Ipv6, ipv6 ? 1 : 0);
        if (Conflate is { } conflate) socket.SetOption(SocketOption.Conflate, conflate ? 1 : 0);
        if (RouterRaw is { } raw) socket.SetOption(SocketOption.RouterRaw, raw ? 1 : 0);
        if (ProbeRouter is { } probe) socket.SetOption(SocketOption.ProbeRouter, probe ? 1 : 0);
        if (ReqCorrelate is { } correlate) socket.SetOption(SocketOption.ReqCorrelate, correlate ? 1 : 0);
        if (ReqRelaxed is { } relaxed) socket.SetOption(SocketOption.ReqRelaxed, relaxed ? 1 : 0);
        if (RouterHandover is { } handover) socket.SetOption(SocketOption.RouterHandover, handover ? 1 : 0);
        if (XPubVerbose is { } verbose) socket.SetOption(SocketOption.XPubVerbose, verbose ? 1 : 0);
        if (XPubNoDrop is { } noDrop) socket.SetOption(SocketOption.XPubNoDrop, noDrop ? 1 : 0);
        if (XPubManual is { } manual) socket.SetOption(SocketOption.XPubManual, manual ? 1 : 0);
        if (XPubVerboser is { } verboser) socket.SetOption(SocketOption.XPubVerboser, verboser ? 1 : 0);
        if (InvertMatching is { } invert) socket.SetOption(SocketOption.InvertMatching, invert ? 1 : 0);
        if (StreamNotify is { } notify) socket.SetOption(SocketOption.StreamNotify, notify ? 1 : 0);
        if (OnlyFirstSubscribe is { } onlyFirst) socket.SetOption(SocketOption.OnlyFirstSubscribe, onlyFirst ? 1 : 0);
        if (MulticastLoop is { } loop) socket.SetOption(SocketOption.MulticastLoop, loop ? 1 : 0);
        if (ZapEnforceDomain is { } enforce) socket.SetOption(SocketOption.ZapEnforceDomain, enforce ? 1 : 0);
        if (ConnectTimeout is { } connectTimeout) socket.SetOption(SocketOption.ConnectTimeout, connectTimeout);
        if (TcpMaxRt is { } tcpMaxRt) socket.SetOption(SocketOption.TcpMaxRt, tcpMaxRt);
        if (ZapDomain is { } zapDomain) socket.SetOption(SocketOption.ZapDomain, zapDomain);
        if (ConnectRoutingId is { } connectRoutingId) socket.SetOption(SocketOption.ConnectRoutingId, connectRoutingId);
        if (GssapiPrincipal is { } principal) socket.SetOption(SocketOption.GssapiPrincipal, principal);
        if (GssapiServicePrincipal is { } service) socket.SetOption(SocketOption.GssapiServicePrincipal, service);
        if (SocksProxy is { } socksProxy) socket.SetOption(SocketOption.SocksProxy, socksProxy);
        if (SocksUsername is { } socksUsername) socket.SetOption(SocketOption.SocksUsername, socksUsername);
        if (SocksPassword is { } socksPassword) socket.SetOption(SocketOption.SocksPassword, socksPassword);
        if (WssKeyPem is { } wssKey) socket.SetOption(SocketOption.WssKeyPem, wssKey);
        if (WssCertPem is { } wssCert) socket.SetOption(SocketOption.WssCertPem, wssCert);
        if (WssTrustPem is { } wssTrust) socket.SetOption(SocketOption.WssTrustPem, wssTrust);
        if (WssHostname is { } hostname) socket.SetOption(SocketOption.WssHostname, hostname);
        if (WssTrustSystem is { } trustSystem) socket.SetOption(SocketOption.WssTrustSystem, trustSystem ? 1 : 0);
        if (HeartbeatInterval is { } heartbeat) socket.SetOption(SocketOption.HeartbeatInterval, heartbeat);
        if (HeartbeatTtl is { } heartbeatTtl) socket.SetOption(SocketOption.HeartbeatTtl, heartbeatTtl);
        if (HeartbeatTimeout is { } heartbeatTimeout) socket.SetOption(SocketOption.HeartbeatTimeout, heartbeatTimeout);
        if (TcpKeepalive is { } keepalive) socket.SetOption(SocketOption.TcpKeepalive, keepalive);
        if (TcpKeepaliveCount is { } keepaliveCount) socket.SetOption(SocketOption.TcpKeepaliveCount, keepaliveCount);
        if (TcpKeepaliveIdle is { } keepaliveIdle) socket.SetOption(SocketOption.TcpKeepaliveIdle, keepaliveIdle);
        if (TcpKeepaliveInterval is { } keepaliveInterval) socket.SetOption(SocketOption.TcpKeepaliveInterval, keepaliveInterval);
    }

    private static int ToMilliseconds(TimeSpan value) => checked((int)value.TotalMilliseconds);
}
