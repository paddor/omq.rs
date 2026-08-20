namespace Omq;

/// OMQ socket patterns supported by the native ABI.
public enum SocketType
{
    Pair = 0, Pub = 1, Sub = 2, Req = 3, Rep = 4, Dealer = 5,
    Router = 6, Pull = 7, Push = 8, XPub = 9, XSub = 10, Stream = 11,
    Server = 12, Client = 13, Radio = 14, Dish = 15, Gather = 16,
    Scatter = 17, Dgram = 18, Peer = 19, Channel = 20
}
