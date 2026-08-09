package omq

type SocketType int32

const (
	Pair SocketType = iota + 1
	Pub
	Sub
	Req
	Rep
	Dealer
	Router
	Pull
	Push
	XPub
	XSub
	Stream
	Server
	Client
	Radio
	Dish
	Gather
	Scatter
	Peer
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

type OverrunPolicy int

const (
	OverrunBlock OverrunPolicy = iota
	OverrunDropOldest
	OverrunDropNewest
	OverrunReturnError
)

type ShareKey struct {
	High uint64
	Low  uint64
}
