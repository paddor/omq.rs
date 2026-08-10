package omq

import (
	"sync/atomic"
	"unsafe"
)

const (
	defaultRecvRingDescCapacity    = 1024
	defaultRecvRingPayloadCapacity = 4 * 1024 * 1024
	recvRingFillBatch              = 512

	recvRingControlHead = 0
	recvRingControlTail = 128

	recvRingFlagMultipart = 1
	recvRingFlagExternal  = 2
)

type recvRingDesc struct {
	payload    uint64
	payloadLen uint64
	totalLen   uint64
	partCount  uint64
	flags      uint64
	payloadEnd uint64
	_          [2]uint64
}

type recvRing struct {
	handle       *nativeRecvRing
	control      unsafe.Pointer
	descriptors  []recvRingDesc
	payload      []byte
	descMask     uint64
	head         uint64
	releasedHead uint64
	cachedTail   uint64
	viewActive   bool
}

func newRecvRing(socket *nativeSocket, ringSize int) (*recvRing, error) {
	descCapacity := defaultRecvRingDescCapacity
	if ringSize > 0 {
		descCapacity = ringSize
	}
	handle, memory, err := recvRingCreateNative(
		socket,
		descCapacity,
		defaultRecvRingPayloadCapacity,
	)
	if err != nil {
		return nil, err
	}
	if memory.control == nil || memory.descriptors == nil || memory.payload == nil ||
		memory.descCapacity <= 0 || memory.payloadCapacity <= 0 ||
		!isPowerOfTwo(uint64(memory.descCapacity)) ||
		!isPowerOfTwo(uint64(memory.payloadCapacity)) {
		recvRingCloseNative(handle)
		return nil, &ConfigError{Err: "native receive ring returned invalid memory"}
	}
	return &recvRing{
		handle:      handle,
		control:     memory.control,
		descriptors: unsafe.Slice((*recvRingDesc)(memory.descriptors), memory.descCapacity),
		payload:     unsafe.Slice((*byte)(memory.payload), memory.payloadCapacity),
		descMask:    uint64(memory.descCapacity - 1),
	}, nil
}

func (r *recvRing) tryRecvView() (RecvView, error) {
	if r == nil || r.handle == nil {
		return RecvView{}, ErrClosed
	}
	r.releaseActiveView()
	if err := r.fillIfEmpty(0); err != nil {
		return RecvView{}, err
	}
	desc := r.current()
	if desc.partCount != 1 {
		r.advance()
		return RecvView{}, &ConfigError{Err: "RecvView requires a single-part message"}
	}
	if desc.flags&recvRingFlagExternal != 0 {
		r.advance()
		return RecvView{}, &ConfigError{Err: "RecvView payload exceeds receive ring capacity"}
	}
	r.viewActive = true
	return RecvView{data: r.source(desc)}, nil
}

func (r *recvRing) tryRecvInto(dst []byte) (int, error) {
	return r.recvInto(dst, 0)
}

func (r *recvRing) recvIntoBlocking(dst []byte) (int, error) {
	return r.recvInto(dst, -1)
}

func (r *recvRing) recvInto(dst []byte, timeoutMillis int64) (int, error) {
	if r == nil || r.handle == nil {
		return 0, ErrClosed
	}
	r.releaseActiveView()
	if err := r.fillIfEmpty(timeoutMillis); err != nil {
		return 0, err
	}
	desc := r.current()
	defer r.advance()
	if desc.partCount != 1 {
		return 0, &ConfigError{Err: "RecvInto requires a single-part message"}
	}
	if desc.flags&recvRingFlagExternal != 0 {
		return 0, &ConfigError{Err: "RecvInto payload exceeds receive ring capacity"}
	}
	if desc.payloadLen > uint64(len(dst)) {
		return 0, &MessageTooLargeError{Err: "destination buffer too small"}
	}
	body := r.source(desc)
	copy(dst, body)
	return len(body), nil
}

func (r *recvRing) close() {
	if r == nil || r.handle == nil {
		return
	}
	r.releaseActiveView()
	r.releaseConsumed()
	recvRingCloseNative(r.handle)
	r.handle = nil
	r.control = nil
	r.descriptors = nil
	r.payload = nil
}

func (r *recvRing) fillIfEmpty(timeoutMillis int64) error {
	if r.hasCached() {
		return nil
	}
	r.releaseConsumed()
	if err := recvRingFillNative(r.handle, timeoutMillis, recvRingFillBatch); err != nil {
		return err
	}
	r.cachedTail = r.tailAcquire()
	if !r.hasCached() {
		return ErrAgain
	}
	return nil
}

func (r *recvRing) hasCached() bool {
	if r.head != r.cachedTail {
		return true
	}
	r.cachedTail = r.tailAcquire()
	return r.head != r.cachedTail
}

func (r *recvRing) current() recvRingDesc {
	return r.descriptors[r.head&r.descMask]
}

func (r *recvRing) source(desc recvRingDesc) []byte {
	if desc.payloadLen == 0 {
		return nil
	}
	return r.payload[desc.payload : desc.payload+desc.payloadLen]
}

func (r *recvRing) releaseActiveView() {
	if r.viewActive {
		r.advance()
		r.viewActive = false
	}
}

func (r *recvRing) advance() {
	r.head++
	if r.head == r.cachedTail {
		r.releaseConsumed()
	}
}

func (r *recvRing) releaseConsumed() {
	if r.releasedHead == r.head {
		return
	}
	atomic.StoreUint64(r.headPtr(), r.head)
	r.releasedHead = r.head
}

func (r *recvRing) tailAcquire() uint64 {
	return atomic.LoadUint64(r.tailPtr())
}

func (r *recvRing) headPtr() *uint64 {
	return (*uint64)(unsafe.Add(r.control, recvRingControlHead))
}

func (r *recvRing) tailPtr() *uint64 {
	return (*uint64)(unsafe.Add(r.control, recvRingControlTail))
}
