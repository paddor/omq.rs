package omq

import (
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	defaultSendRingDescCapacity    = 4096
	defaultSendRingPayloadCapacity = 16 * 1024 * 1024

	sendRingControlHead   = 0
	sendRingControlTail   = 128
	sendRingControlClosed = 256
)

type sendRingDesc struct {
	payload    uint64
	payloadLen uint64
	payloadEnd uint64
	_          [5]uint64
}

type sendRing struct {
	handle        *nativeSendRing
	control       unsafe.Pointer
	descriptors   []sendRingDesc
	payload       []byte
	descMask      uint64
	payloadMask   uint64
	tail          uint64
	cachedHead    uint64
	reclaimedHead uint64
	payloadTail   uint64
	payloadHead   uint64
}

func newSendRing(socket *nativeSocket, ringSize int) (*sendRing, error) {
	descCapacity := defaultSendRingDescCapacity
	if ringSize > 0 {
		descCapacity = ringSize
	}
	handle, memory, err := sendRingCreateNative(
		socket,
		descCapacity,
		defaultSendRingPayloadCapacity,
	)
	if err != nil {
		return nil, err
	}
	if memory.control == nil || memory.descriptors == nil || memory.payload == nil ||
		memory.descCapacity <= 0 || memory.payloadCapacity <= 0 ||
		!isPowerOfTwo(uint64(memory.descCapacity)) ||
		!isPowerOfTwo(uint64(memory.payloadCapacity)) {
		sendRingCloseNative(handle)
		return nil, &ConfigError{Err: "native send ring returned invalid memory"}
	}
	return &sendRing{
		handle:      handle,
		control:     memory.control,
		descriptors: unsafe.Slice((*sendRingDesc)(memory.descriptors), memory.descCapacity),
		payload:     unsafe.Slice((*byte)(memory.payload), memory.payloadCapacity),
		descMask:    uint64(memory.descCapacity - 1),
		payloadMask: uint64(memory.payloadCapacity - 1),
	}, nil
}

func (r *sendRing) trySend(body []byte) (bool, error) {
	if r == nil || r.handle == nil {
		return false, nil
	}
	if len(body) >= len(r.payload) {
		if ok, err := r.drain(-1); err != nil || !ok {
			return true, err
		}
		return false, nil
	}
	if r.closedAcquire() {
		return true, r.error()
	}
	if r.descIsFull() {
		return true, ErrAgain
	}
	reservation, ok := r.reservePayload(len(body))
	if !ok {
		r.reclaimConsumed()
		reservation, ok = r.reservePayload(len(body))
		if !ok {
			return true, ErrAgain
		}
	}

	if len(body) > 0 {
		copy(r.payload[reservation.offset:reservation.offset+uint64(len(body))], body)
	}
	desc := &r.descriptors[r.tail&r.descMask]
	desc.payload = reservation.offset
	desc.payloadLen = uint64(len(body))
	desc.payloadEnd = reservation.end
	r.tail++
	atomic.StoreUint64(r.tailPtr(), r.tail)
	runtime.KeepAlive(body)
	return true, nil
}

func (r *sendRing) close() error {
	if r == nil || r.handle == nil {
		return nil
	}
	_, err := r.drain(0)
	sendRingCloseNative(r.handle)
	r.handle = nil
	r.control = nil
	r.descriptors = nil
	r.payload = nil
	return err
}

func (r *sendRing) drain(timeoutMillis int64) (bool, error) {
	if r == nil || r.handle == nil {
		return true, nil
	}
	start := time.Now()
	timeout := saturatedNanos(timeoutMillis)
	spins := 0
	for r.tail != r.headAcquire() {
		if r.closedAcquire() {
			return false, r.error()
		}
		if timeoutMillis == 0 {
			return false, nil
		}
		if timeoutMillis > 0 && time.Since(start) >= timeout {
			return false, nil
		}
		spins = sendRingBackoff(spins)
	}
	r.reclaimConsumed()
	return true, nil
}

func (r *sendRing) descIsFull() bool {
	if r.tail-r.cachedHead < uint64(len(r.descriptors)) {
		return false
	}
	r.cachedHead = r.headAcquire()
	return r.tail-r.cachedHead >= uint64(len(r.descriptors))
}

func (r *sendRing) reservePayload(length int) (sendRingReservation, bool) {
	if length == 0 {
		return sendRingReservation{offset: 0, end: r.payloadTail}, true
	}
	cursor := r.payloadTail
	offset := cursor & r.payloadMask
	needed := uint64(length)
	if offset+uint64(length) > uint64(len(r.payload)) {
		pad := uint64(len(r.payload)) - offset
		cursor += pad
		needed += pad
		offset = 0
	}
	if cursor+uint64(length)-r.payloadHead > uint64(len(r.payload)) {
		return sendRingReservation{}, false
	}
	r.payloadTail += needed
	return sendRingReservation{offset: offset, end: cursor + uint64(length)}, true
}

func (r *sendRing) reclaimConsumed() {
	head := r.headAcquire()
	for r.reclaimedHead != head {
		desc := r.descriptors[r.reclaimedHead&r.descMask]
		r.payloadHead = desc.payloadEnd
		r.reclaimedHead++
	}
	r.cachedHead = head
}

func (r *sendRing) headAcquire() uint64 {
	return atomic.LoadUint64(r.headPtr())
}

func (r *sendRing) closedAcquire() bool {
	return atomic.LoadUint64(r.closedPtr()) != 0
}

func (r *sendRing) error() error {
	if err := sendRingErrorNative(r.handle); err != nil {
		return err
	}
	return ErrClosed
}

func (r *sendRing) headPtr() *uint64 {
	return (*uint64)(unsafe.Add(r.control, sendRingControlHead))
}

func (r *sendRing) tailPtr() *uint64 {
	return (*uint64)(unsafe.Add(r.control, sendRingControlTail))
}

func (r *sendRing) closedPtr() *uint64 {
	return (*uint64)(unsafe.Add(r.control, sendRingControlClosed))
}

type sendRingReservation struct {
	offset uint64
	end    uint64
}

func sendRingBackoff(spins int) int {
	if spins < 256 {
		runtime.Gosched()
		return spins + 1
	}
	if spins < 512 {
		runtime.Gosched()
		return spins + 1
	}
	time.Sleep(50 * time.Microsecond)
	return spins
}

func saturatedNanos(timeoutMillis int64) time.Duration {
	if timeoutMillis <= 0 {
		return time.Duration(timeoutMillis)
	}
	maxMillis := int64(1<<63-1) / int64(time.Millisecond)
	if timeoutMillis >= maxMillis {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(timeoutMillis) * time.Millisecond
}

func isPowerOfTwo(value uint64) bool {
	return value > 0 && value&(value-1) == 0
}
