package omq

// SocketType identifies an OMQ socket type.
type SocketType int32

const (
	// Pair is a bidirectional exclusive 1:1 socket.
	Pair SocketType = iota + 1
	// Pub publishes topic-prefixed messages to Sub sockets.
	Pub
	// Sub receives subscribed topic prefixes from Pub sockets.
	Sub
	// Req sends one request and then receives one reply.
	Req
	// Rep receives one request and then sends one reply.
	Rep
	// Dealer is an async request/reply socket without the REQ FSM.
	Dealer
	// Router routes multipart messages by leading identity frame.
	Router
	// Pull fair-queues messages from Push sockets.
	Pull
	// Push round-robins messages to Pull sockets.
	Push
	// XPub exposes raw subscription frames.
	XPub
	// XSub sends raw subscription frames upstream.
	XSub
	// Stream bridges raw TCP connections.
	Stream
	// Server is a single-part ROUTER-style socket.
	Server
	// Client is a single-part DEALER-style socket.
	Client
	// Radio publishes group-addressed messages to Dish sockets.
	Radio
	// Dish receives joined groups from Radio sockets.
	Dish
	// Gather fair-queues single-part messages from Scatter sockets.
	Gather
	// Scatter round-robins single-part messages to Gather sockets.
	Scatter
	// Peer is a bidirectional identity-routed peer socket.
	Peer
	// Channel is a bidirectional single-part PAIR-style socket.
	Channel
)

func (t SocketType) String() string {
	switch t {
	case Pair:
		return "PAIR"
	case Pub:
		return "PUB"
	case Sub:
		return "SUB"
	case Req:
		return "REQ"
	case Rep:
		return "REP"
	case Dealer:
		return "DEALER"
	case Router:
		return "ROUTER"
	case Pull:
		return "PULL"
	case Push:
		return "PUSH"
	case XPub:
		return "XPUB"
	case XSub:
		return "XSUB"
	case Stream:
		return "STREAM"
	case Server:
		return "SERVER"
	case Client:
		return "CLIENT"
	case Radio:
		return "RADIO"
	case Dish:
		return "DISH"
	case Gather:
		return "GATHER"
	case Scatter:
		return "SCATTER"
	case Peer:
		return "PEER"
	case Channel:
		return "CHANNEL"
	default:
		return "UNKNOWN"
	}
}

func (t SocketType) canSend() bool {
	switch t {
	case Sub, Pull, Gather, Dish:
		return false
	default:
		return true
	}
}

func (t SocketType) canRecv() bool {
	switch t {
	case Pub, Push, Scatter, Radio:
		return false
	default:
		return true
	}
}

// OverrunPolicy controls how channel adapters handle full Go channels.
type OverrunPolicy int

const (
	// OverrunBlock blocks until channel space is available.
	OverrunBlock OverrunPolicy = iota
	// OverrunDropOldest drops the oldest channel item.
	OverrunDropOldest
	// OverrunDropNewest drops the newest incoming item.
	OverrunDropNewest
	// OverrunReturnError reports an overrun through the error channel.
	OverrunReturnError
)

// ShareKey identifies a process-local shared native context.
type ShareKey struct {
	// High is the high 64 bits of the share key.
	High uint64
	// Low is the low 64 bits of the share key.
	Low uint64
}

// CurveKeypair is a Z85 CURVE public/secret key pair.
type CurveKeypair struct {
	// Public is the Z85 public key.
	Public string
	// Secret is the Z85 secret key.
	Secret string
}

// PeerInfo describes a peer seen by auth callbacks or monitor events.
type PeerInfo struct {
	// Mechanism is the ZMTP mechanism name.
	Mechanism string
	// PublicKey is the peer CURVE public key.
	PublicKey string
	// Identity is the peer routing identity.
	Identity []byte
	// Username is the PLAIN username.
	Username string
	// Password is the PLAIN password.
	Password string
	// ConnectionID is the native per-socket connection id.
	ConnectionID uint64
	// PeerAddress is the remote TCP address when known.
	PeerAddress string
	// SocketType is the peer READY socket type when known.
	SocketType string
	// ZMTPMajor is the negotiated ZMTP major version.
	ZMTPMajor uint8
	// ZMTPMinor is the negotiated ZMTP minor version.
	ZMTPMinor uint8
}

// WorkloadProfile selects native throughput or latency tuning.
type WorkloadProfile int32

const (
	// WorkloadThroughput favors batching and throughput.
	WorkloadThroughput WorkloadProfile = iota
	// WorkloadLatency favors lower latency.
	WorkloadLatency
)

// OnMute controls native send behavior when outbound queues are full.
type OnMute int32

const (
	// OnMuteBlock waits for queue capacity.
	OnMuteBlock OnMute = iota
	// OnMuteDropNewest drops the newest message on mute.
	OnMuteDropNewest
	// OnMuteDropOldest drops the oldest queued message on mute.
	OnMuteDropOldest
)
