package omq

/*
#cgo CFLAGS: -I${SRCDIR}/native/include
#cgo linux LDFLAGS: -L${SRCDIR}/native/target/release -L${SRCDIR}/native/target/debug -lomq_go -Wl,-rpath,${SRCDIR}/native/target/release -Wl,-rpath,${SRCDIR}/native/target/debug
#cgo darwin LDFLAGS: -L${SRCDIR}/native/target/release -L${SRCDIR}/native/target/debug -lomq_go -Wl,-rpath,${SRCDIR}/native/target/release -Wl,-rpath,${SRCDIR}/native/target/debug
#include "omq_go.h"
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"time"
	"unsafe"
)

type nativeContext = C.OmqGoContext
type nativeSocket = C.OmqGoSocket
type nativeMonitor = C.OmqGoMonitor
type nativeSendRing = C.OmqGoSendRing
type nativeRecvRing = C.OmqGoRecvRing
type nativeCancel = C.OmqGoCancel

type nativeStats struct {
	contextsCreated  uint64
	contextsFreed    uint64
	contextsLive     uint64
	socketsCreated   uint64
	socketsFreed     uint64
	socketsLive      uint64
	monitorsCreated  uint64
	monitorsFreed    uint64
	monitorsLive     uint64
	sendRingsCreated uint64
	sendRingsFreed   uint64
	sendRingsLive    uint64
	recvRingsCreated uint64
	recvRingsFreed   uint64
	recvRingsLive    uint64
	cancelsCreated   uint64
	cancelsFreed     uint64
	cancelsLive      uint64
}

type sendRingMemory struct {
	control         unsafe.Pointer
	descriptors     unsafe.Pointer
	payload         unsafe.Pointer
	descCapacity    int
	payloadCapacity int
}

type recvRingMemory struct {
	control         unsafe.Pointer
	descriptors     unsafe.Pointer
	payload         unsafe.Pointer
	descCapacity    int
	payloadCapacity int
}

func statusErr(status C.OmqGoStatus) error {
	if status.code == C.OMQ_GO_OK {
		return nil
	}
	msg := "omq error"
	if status.message != nil {
		msg = C.GoString(status.message)
		C.omq_go_string_free(status.message)
	}
	switch status.code {
	case C.OMQ_GO_AGAIN:
		return ErrAgain
	case C.OMQ_GO_CLOSED:
		return ErrClosed
	case C.OMQ_GO_TIMEOUT:
		return ErrTimeout
	case C.OMQ_GO_CANCELED:
		return ErrCanceled
	case C.OMQ_GO_INVALID_ENDPOINT:
		return &EndpointError{Op: "endpoint", Err: msg}
	case C.OMQ_GO_UNSUPPORTED_SCHEME:
		return &EndpointError{Op: "scheme", Err: msg}
	case C.OMQ_GO_PROTOCOL:
		return &ProtocolError{Err: msg}
	case C.OMQ_GO_CONFIG:
		return &ConfigError{Err: msg}
	case C.OMQ_GO_IO:
		return &TransportError{Err: msg}
	case C.OMQ_GO_UNROUTABLE:
		return ErrUnroutable
	case C.OMQ_GO_MESSAGE_TOO_LARGE:
		return &MessageTooLargeError{Err: msg}
	default:
		return &Error{Err: msg}
	}
}

func contextOpenNative(ioThreads int) (*nativeContext, error) {
	var out *C.OmqGoContext
	err := statusErr(C.omq_go_context_open(C.size_t(ioThreads), &out))
	if err != nil {
		return nil, err
	}
	return (*nativeContext)(out), nil
}

func nativeStatsNative() nativeStats {
	var out C.OmqGoNativeStats
	C.omq_go_native_stats(&out)
	return nativeStats{
		contextsCreated:  uint64(out.contexts_created),
		contextsFreed:    uint64(out.contexts_freed),
		contextsLive:     uint64(out.contexts_live),
		socketsCreated:   uint64(out.sockets_created),
		socketsFreed:     uint64(out.sockets_freed),
		socketsLive:      uint64(out.sockets_live),
		monitorsCreated:  uint64(out.monitors_created),
		monitorsFreed:    uint64(out.monitors_freed),
		monitorsLive:     uint64(out.monitors_live),
		sendRingsCreated: uint64(out.send_rings_created),
		sendRingsFreed:   uint64(out.send_rings_freed),
		sendRingsLive:    uint64(out.send_rings_live),
		recvRingsCreated: uint64(out.recv_rings_created),
		recvRingsFreed:   uint64(out.recv_rings_freed),
		recvRingsLive:    uint64(out.recv_rings_live),
		cancelsCreated:   uint64(out.cancels_created),
		cancelsFreed:     uint64(out.cancels_freed),
		cancelsLive:      uint64(out.cancels_live),
	}
}

func contextFromShareKeyNative(key ShareKey) (*nativeContext, error) {
	var out *C.OmqGoContext
	err := statusErr(C.omq_go_context_from_share_key(C.uint64_t(key.High), C.uint64_t(key.Low), &out))
	if err != nil {
		return nil, err
	}
	return (*nativeContext)(out), nil
}

func contextShareKeyNative(ctx *nativeContext) (ShareKey, error) {
	var high C.uint64_t
	var low C.uint64_t
	err := statusErr(C.omq_go_context_share_key((*C.OmqGoContext)(ctx), &high, &low))
	if err != nil {
		return ShareKey{}, err
	}
	return ShareKey{High: uint64(high), Low: uint64(low)}, nil
}

func contextCloseNative(ctx *nativeContext) {
	C.omq_go_context_close((*C.OmqGoContext)(ctx))
}

func contextFreeNative(ctx *nativeContext) {
	C.omq_go_context_free((*C.OmqGoContext)(ctx))
}

func curveKeypairNative() (CurveKeypair, error) {
	var publicKey *C.char
	var secretKey *C.char
	err := statusErr(C.omq_go_curve_keypair(&publicKey, &secretKey))
	if err != nil {
		return CurveKeypair{}, err
	}
	defer C.omq_go_string_free(publicKey)
	defer C.omq_go_string_free(secretKey)
	return CurveKeypair{
		Public: C.GoString(publicKey),
		Secret: C.GoString(secretKey),
	}, nil
}

func curvePublicNative(secretKey string) (string, error) {
	cSecret := C.CString(secretKey)
	defer C.free(unsafe.Pointer(cSecret))
	var publicKey *C.char
	err := statusErr(C.omq_go_curve_public(cSecret, &publicKey))
	if err != nil {
		return "", err
	}
	defer C.omq_go_string_free(publicKey)
	return C.GoString(publicKey), nil
}

//export omqGoAuthCallback
func omqGoAuthCallback(id C.uint64_t, peer *C.OmqGoAuthPeer) C.int {
	if peer == nil {
		return 0
	}
	if callAuthCallback(uint64(id), peerInfoFromC(peer)) {
		return 1
	}
	return 0
}

func socketNewNative(ctx *nativeContext, socketType SocketType) (*nativeSocket, error) {
	var out *C.OmqGoSocket
	err := statusErr(C.omq_go_socket_new((*C.OmqGoContext)(ctx), C.int32_t(socketType), &out))
	if err != nil {
		return nil, err
	}
	return (*nativeSocket)(out), nil
}

func socketBindNative(socket *nativeSocket, endpoint string) (string, error) {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	var bound *C.char
	err := statusErr(C.omq_go_socket_bind((*C.OmqGoSocket)(socket), cEndpoint, &bound))
	if err != nil {
		return "", err
	}
	if bound == nil {
		return endpoint, nil
	}
	defer C.omq_go_string_free(bound)
	return C.GoString(bound), nil
}

func socketBindTimeoutNative(socket *nativeSocket, endpoint string, timeoutMillis int64) (string, error) {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	var bound *C.char
	err := statusErr(C.omq_go_socket_bind_timeout((*C.OmqGoSocket)(socket), cEndpoint, &bound, C.int64_t(timeoutMillis)))
	if err != nil {
		return "", err
	}
	if bound == nil {
		return endpoint, nil
	}
	defer C.omq_go_string_free(bound)
	return C.GoString(bound), nil
}

func socketConnectNative(socket *nativeSocket, endpoint string) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	return statusErr(C.omq_go_socket_connect((*C.OmqGoSocket)(socket), cEndpoint))
}

func socketConnectTimeoutNative(socket *nativeSocket, endpoint string, timeoutMillis int64) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	return statusErr(C.omq_go_socket_connect_timeout((*C.OmqGoSocket)(socket), cEndpoint, C.int64_t(timeoutMillis)))
}

func socketUnbindNative(socket *nativeSocket, endpoint string) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	return statusErr(C.omq_go_socket_unbind((*C.OmqGoSocket)(socket), cEndpoint))
}

func socketDisconnectNative(socket *nativeSocket, endpoint string) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	return statusErr(C.omq_go_socket_disconnect((*C.OmqGoSocket)(socket), cEndpoint))
}

func socketMessageSendNative(socket *nativeSocket, msg Message) error {
	return socketMessageSendNativeTimeout(socket, msg, 0)
}

func socketMessageSendWaitNative(socket *nativeSocket, msg Message) error {
	return socketMessageSendNativeTimeout(socket, msg, -1)
}

func socketMessageSendNativeTimeout(socket *nativeSocket, msg Message, timeoutMillis int64) error {
	if parts := msg.partsView(); len(parts) == 1 {
		part := parts[0]
		if len(part) == 0 {
			return statusErr(C.omq_go_socket_send_one((*C.OmqGoSocket)(socket), nil, 0, C.uint32_t(msg.routingID), C.int64_t(timeoutMillis)))
		}
		status := C.omq_go_socket_send_one(
			(*C.OmqGoSocket)(socket),
			(*C.uint8_t)(unsafe.Pointer(&part[0])),
			C.size_t(len(part)),
			C.uint32_t(msg.routingID),
			C.int64_t(timeoutMillis),
		)
		runtime.KeepAlive(part)
		return statusErr(status)
	}
	parts, count, free := messageToC(msg)
	defer free()
	return statusErr(C.omq_go_socket_send((*C.OmqGoSocket)(socket), parts, count, C.uint32_t(msg.routingID), C.int64_t(timeoutMillis)))
}

func socketMessagesTrySendNative(socket *nativeSocket, messages []Message) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}
	size := C.size_t(len(messages)) * C.size_t(unsafe.Sizeof(C.OmqGoWireMessage{}))
	ptr := C.malloc(size)
	wire := unsafe.Slice((*C.OmqGoWireMessage)(ptr), len(messages))
	freeFns := make([]func(), 0, len(messages))
	for i, msg := range messages {
		parts, count, free := messageToC(msg)
		freeFns = append(freeFns, free)
		wire[i].parts = parts
		wire[i].part_count = count
		wire[i].routing_id = C.uint32_t(msg.routingID)
	}
	defer func() {
		for _, free := range freeFns {
			free()
		}
		C.free(ptr)
	}()
	var sent C.size_t
	err := statusErr(C.omq_go_socket_try_send_batch((*C.OmqGoSocket)(socket), (*C.OmqGoWireMessage)(ptr), C.size_t(len(messages)), &sent))
	return int(sent), err
}

func receiveAnyNative(sockets []*Socket, timeoutMillis int64) (ReceiveEvent, error) {
	if len(sockets) == 0 {
		return ReceiveEvent{}, &ConfigError{Err: "receive-any requires at least one socket"}
	}
	nativeHandles, unlock, err := lockNativeSockets(sockets)
	if err != nil {
		return ReceiveEvent{}, err
	}
	defer unlock()

	size := C.size_t(len(sockets)) * C.size_t(unsafe.Sizeof(uintptr(0)))
	ptr := C.malloc(size)
	if ptr == nil {
		return ReceiveEvent{}, &Error{Err: "receive-any allocation failed"}
	}
	defer C.free(ptr)

	handles := unsafe.Slice((**C.OmqGoSocket)(ptr), len(sockets))
	for i, handle := range nativeHandles {
		handles[i] = (*C.OmqGoSocket)(handle)
	}

	var index C.size_t
	var out C.OmqGoMessage
	err = statusErr(C.omq_go_receive_any(
		(**C.OmqGoSocket)(ptr),
		C.size_t(len(sockets)),
		C.int64_t(timeoutMillis),
		&index,
		&out,
	))
	if err != nil {
		return ReceiveEvent{}, err
	}
	defer C.omq_go_message_free(out)
	goIndex := int(index)
	if goIndex < 0 || goIndex >= len(sockets) {
		return ReceiveEvent{}, &Error{Err: "native receive-any returned invalid socket index"}
	}
	keepAlive(sockets)
	return ReceiveEvent{Socket: sockets[goIndex], Message: messageFromC(out)}, nil
}

func lockNativeSockets(sockets []*Socket) ([]*nativeSocket, func(), error) {
	order := append([]*Socket(nil), sockets...)
	sort.Slice(order, func(i, j int) bool {
		return uintptr(unsafe.Pointer(order[i])) < uintptr(unsafe.Pointer(order[j]))
	})

	locked := make([]*socketState, 0, len(order))
	unlock := func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].handleMu.RUnlock()
		}
	}
	for _, socket := range order {
		if socket == nil {
			unlock()
			return nil, nil, &ConfigError{Err: "receive-any socket is nil"}
		}
		state := socket.stateOrNil()
		if state == nil {
			unlock()
			return nil, nil, ErrClosed
		}
		state.handleMu.RLock()
		if state.closed.Load() || state.handle == nil {
			state.handleMu.RUnlock()
			unlock()
			return nil, nil, ErrClosed
		}
		locked = append(locked, state)
	}

	handles := make([]*nativeSocket, len(sockets))
	for i, socket := range sockets {
		handles[i] = socket.state.handle
	}
	return handles, unlock, nil
}

func socketMessageRecvNative(socket *nativeSocket) (Message, error) {
	var out C.OmqGoMessage
	err := statusErr(C.omq_go_socket_recv((*C.OmqGoSocket)(socket), 0, &out))
	if err != nil {
		return Message{}, err
	}
	defer C.omq_go_message_free(out)
	return messageFromC(out), nil
}

func socketMessageRecvCancelableNative(socket *nativeSocket, cancel *nativeCancel) (Message, error) {
	var out C.OmqGoMessage
	err := statusErr(C.omq_go_socket_recv_cancelable(
		(*C.OmqGoSocket)(socket),
		(*C.OmqGoCancel)(cancel),
		&out,
	))
	if err != nil {
		return Message{}, err
	}
	defer C.omq_go_message_free(out)
	return messageFromC(out), nil
}

func socketMessageRecvIntoNative(socket *nativeSocket, dst []byte) (int, error) {
	return socketMessageRecvIntoNativeTimeout(socket, dst, 0)
}

func socketMessageRecvIntoCancelableNative(socket *nativeSocket, cancel *nativeCancel, dst []byte) (int, error) {
	var written C.size_t
	var data *C.uint8_t
	if len(dst) > 0 {
		data = (*C.uint8_t)(unsafe.Pointer(&dst[0]))
	}
	err := statusErr(C.omq_go_socket_recv_one_into_cancelable(
		(*C.OmqGoSocket)(socket),
		(*C.OmqGoCancel)(cancel),
		data,
		C.size_t(len(dst)),
		&written,
	))
	runtime.KeepAlive(dst)
	if err != nil {
		return 0, err
	}
	return int(written), nil
}

func socketMessageRecvIntoNativeTimeout(socket *nativeSocket, dst []byte, timeoutMillis int64) (int, error) {
	var written C.size_t
	var data *C.uint8_t
	if len(dst) > 0 {
		data = (*C.uint8_t)(unsafe.Pointer(&dst[0]))
	}
	err := statusErr(C.omq_go_socket_recv_one_into(
		(*C.OmqGoSocket)(socket),
		C.int64_t(timeoutMillis),
		data,
		C.size_t(len(dst)),
		&written,
	))
	runtime.KeepAlive(dst)
	if err != nil {
		return 0, err
	}
	return int(written), nil
}

func sendRingCreateNative(socket *nativeSocket, descCapacity, payloadCapacity int) (*nativeSendRing, sendRingMemory, error) {
	var out *C.OmqGoSendRing
	err := statusErr(C.omq_go_send_ring_create(
		(*C.OmqGoSocket)(socket),
		C.size_t(descCapacity),
		C.size_t(payloadCapacity),
		&out,
	))
	if err != nil {
		return nil, sendRingMemory{}, err
	}
	var memory C.OmqGoSendRingMemory
	err = statusErr(C.omq_go_send_ring_memory(out, &memory))
	if err != nil {
		C.omq_go_send_ring_close(out)
		return nil, sendRingMemory{}, err
	}
	return (*nativeSendRing)(out), sendRingMemory{
		control:         unsafe.Pointer(memory.control),
		descriptors:     unsafe.Pointer(memory.descriptors),
		payload:         unsafe.Pointer(memory.payload),
		descCapacity:    int(memory.desc_capacity),
		payloadCapacity: int(memory.payload_capacity),
	}, nil
}

func sendRingErrorNative(ring *nativeSendRing) error {
	return statusErr(C.omq_go_send_ring_error((*C.OmqGoSendRing)(ring)))
}

func sendRingCloseNative(ring *nativeSendRing) {
	C.omq_go_send_ring_close((*C.OmqGoSendRing)(ring))
}

func recvRingCreateNative(socket *nativeSocket, descCapacity, payloadCapacity int) (*nativeRecvRing, recvRingMemory, error) {
	var out *C.OmqGoRecvRing
	err := statusErr(C.omq_go_recv_ring_create(
		(*C.OmqGoSocket)(socket),
		C.size_t(descCapacity),
		C.size_t(payloadCapacity),
		&out,
	))
	if err != nil {
		return nil, recvRingMemory{}, err
	}
	var memory C.OmqGoRecvRingMemory
	err = statusErr(C.omq_go_recv_ring_memory(out, &memory))
	if err != nil {
		C.omq_go_recv_ring_close(out)
		return nil, recvRingMemory{}, err
	}
	return (*nativeRecvRing)(out), recvRingMemory{
		control:         unsafe.Pointer(memory.control),
		descriptors:     unsafe.Pointer(memory.descriptors),
		payload:         unsafe.Pointer(memory.payload),
		descCapacity:    int(memory.desc_capacity),
		payloadCapacity: int(memory.payload_capacity),
	}, nil
}

func recvRingFillNative(ring *nativeRecvRing, timeoutMillis int64, maxMessages int) error {
	return statusErr(C.omq_go_recv_ring_fill(
		(*C.OmqGoRecvRing)(ring),
		C.int64_t(timeoutMillis),
		C.size_t(maxMessages),
	))
}

func recvRingFillCancelableNative(ring *nativeRecvRing, cancel *nativeCancel, maxMessages int) error {
	return statusErr(C.omq_go_recv_ring_fill_cancelable(
		(*C.OmqGoRecvRing)(ring),
		(*C.OmqGoCancel)(cancel),
		C.size_t(maxMessages),
	))
}

func recvRingCloseNative(ring *nativeRecvRing) {
	C.omq_go_recv_ring_close((*C.OmqGoRecvRing)(ring))
}

func cancelNewNative() *nativeCancel {
	return (*nativeCancel)(C.omq_go_cancel_new())
}

func cancelNative(cancel *nativeCancel) {
	C.omq_go_cancel((*C.OmqGoCancel)(cancel))
}

func cancelRegisterCurrentNative(cancel *nativeCancel) {
	C.omq_go_cancel_register_current((*C.OmqGoCancel)(cancel))
}

func cancelFreeNative(cancel *nativeCancel) {
	C.omq_go_cancel_free((*C.OmqGoCancel)(cancel))
}

func socketSubscribeNative(socket *nativeSocket, data []byte) error {
	if len(data) == 0 {
		return statusErr(C.omq_go_socket_subscribe((*C.OmqGoSocket)(socket), nil, 0))
	}
	ptr := C.CBytes(data)
	defer C.free(ptr)
	return statusErr(C.omq_go_socket_subscribe((*C.OmqGoSocket)(socket), (*C.uint8_t)(ptr), C.size_t(len(data))))
}

func socketUnsubscribeNative(socket *nativeSocket, data []byte) error {
	if len(data) == 0 {
		return statusErr(C.omq_go_socket_unsubscribe((*C.OmqGoSocket)(socket), nil, 0))
	}
	ptr := C.CBytes(data)
	defer C.free(ptr)
	return statusErr(C.omq_go_socket_unsubscribe((*C.OmqGoSocket)(socket), (*C.uint8_t)(ptr), C.size_t(len(data))))
}

func socketJoinNative(socket *nativeSocket, data []byte) error {
	if len(data) == 0 {
		return statusErr(C.omq_go_socket_join((*C.OmqGoSocket)(socket), nil, 0))
	}
	ptr := C.CBytes(data)
	defer C.free(ptr)
	return statusErr(C.omq_go_socket_join((*C.OmqGoSocket)(socket), (*C.uint8_t)(ptr), C.size_t(len(data))))
}

func socketLeaveNative(socket *nativeSocket, data []byte) error {
	if len(data) == 0 {
		return statusErr(C.omq_go_socket_leave((*C.OmqGoSocket)(socket), nil, 0))
	}
	ptr := C.CBytes(data)
	defer C.free(ptr)
	return statusErr(C.omq_go_socket_leave((*C.OmqGoSocket)(socket), (*C.uint8_t)(ptr), C.size_t(len(data))))
}

func socketWaitConnectedNative(socket *nativeSocket, minPeers int, timeout time.Duration) (int, error) {
	if minPeers < 0 {
		return 0, &ConfigError{Err: "min peers must be non-negative"}
	}
	var out C.size_t
	err := statusErr(C.omq_go_socket_wait_connected(
		(*C.OmqGoSocket)(socket),
		C.size_t(minPeers),
		C.int64_t(durationMillis(timeout)),
		&out,
	))
	return int(out), err
}

func socketWaitSubscribedNative(socket *nativeSocket, minSubscriptions uint64, timeout time.Duration) (uint64, error) {
	var out C.uint64_t
	err := statusErr(C.omq_go_socket_wait_subscribed(
		(*C.OmqGoSocket)(socket),
		C.uint64_t(minSubscriptions),
		C.int64_t(durationMillis(timeout)),
		&out,
	))
	return uint64(out), err
}

func socketCloseNative(socket *nativeSocket, linger time.Duration, useConfigured bool) error {
	millis := int64(-2)
	if !useConfigured {
		millis = durationMillis(linger)
	}
	return statusErr(C.omq_go_socket_close((*C.OmqGoSocket)(socket), C.int64_t(millis)))
}

func socketFreeNative(socket *nativeSocket) {
	C.omq_go_socket_free((*C.OmqGoSocket)(socket))
}

func setSendHWMNative(socket *nativeSocket, value uint32) error {
	return statusErr(C.omq_go_socket_set_send_hwm((*C.OmqGoSocket)(socket), C.uint32_t(value)))
}

func setRecvHWMNative(socket *nativeSocket, value uint32) error {
	return statusErr(C.omq_go_socket_set_recv_hwm((*C.OmqGoSocket)(socket), C.uint32_t(value)))
}

func setLingerNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_linger((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setIdentityNative(socket *nativeSocket, value []byte) error {
	return socketSetBytesNative(socket, value, func(s *C.OmqGoSocket, p *C.uint8_t, n C.size_t) C.OmqGoStatus {
		return C.omq_go_socket_set_identity(s, p, n)
	})
}

func setHeartbeatIntervalNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_heartbeat_interval((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setHandshakeTimeoutNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_handshake_timeout((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setMaxMessageSizeNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_max_message_size((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setPlainServerNative(socket *nativeSocket, username, password string) error {
	cUsername := C.CString(username)
	defer C.free(unsafe.Pointer(cUsername))
	cPassword := C.CString(password)
	defer C.free(unsafe.Pointer(cPassword))
	return statusErr(C.omq_go_socket_set_plain_server((*C.OmqGoSocket)(socket), cUsername, cPassword))
}

func setPlainServerAuthNative(socket *nativeSocket, callbackID uint64) error {
	return statusErr(C.omq_go_socket_set_plain_server_callback(
		(*C.OmqGoSocket)(socket),
		C.uint64_t(callbackID),
	))
}

func setPlainClientNative(socket *nativeSocket, username, password string) error {
	cUsername := C.CString(username)
	defer C.free(unsafe.Pointer(cUsername))
	cPassword := C.CString(password)
	defer C.free(unsafe.Pointer(cPassword))
	return statusErr(C.omq_go_socket_set_plain_client((*C.OmqGoSocket)(socket), cUsername, cPassword))
}

func setCurveServerNative(socket *nativeSocket, keypair CurveKeypair) error {
	cPublic := C.CString(keypair.Public)
	defer C.free(unsafe.Pointer(cPublic))
	cSecret := C.CString(keypair.Secret)
	defer C.free(unsafe.Pointer(cSecret))
	return statusErr(C.omq_go_socket_set_curve_server((*C.OmqGoSocket)(socket), cPublic, cSecret))
}

func setCurveServerAuthNative(socket *nativeSocket, keypair CurveKeypair, callbackID uint64) error {
	cPublic := C.CString(keypair.Public)
	defer C.free(unsafe.Pointer(cPublic))
	cSecret := C.CString(keypair.Secret)
	defer C.free(unsafe.Pointer(cSecret))
	return statusErr(C.omq_go_socket_set_curve_server_callback(
		(*C.OmqGoSocket)(socket),
		cPublic,
		cSecret,
		C.uint64_t(callbackID),
	))
}

func setCurveClientNative(socket *nativeSocket, keypair CurveKeypair, serverPublicKey string) error {
	cPublic := C.CString(keypair.Public)
	defer C.free(unsafe.Pointer(cPublic))
	cSecret := C.CString(keypair.Secret)
	defer C.free(unsafe.Pointer(cSecret))
	cServerPublic := C.CString(serverPublicKey)
	defer C.free(unsafe.Pointer(cServerPublic))
	return statusErr(C.omq_go_socket_set_curve_client(
		(*C.OmqGoSocket)(socket),
		cPublic,
		cSecret,
		cServerPublic,
	))
}

func setWorkloadProfileNative(socket *nativeSocket, value int32) error {
	return statusErr(C.omq_go_socket_set_workload_profile((*C.OmqGoSocket)(socket), C.int32_t(value)))
}

func setReconnectNative(socket *nativeSocket, mode int32, minMillis, maxMillis int64) error {
	return statusErr(C.omq_go_socket_set_reconnect(
		(*C.OmqGoSocket)(socket),
		C.int32_t(mode),
		C.int64_t(minMillis),
		C.int64_t(maxMillis),
	))
}

func setReconnectStopConnRefusedNative(socket *nativeSocket, enabled bool) error {
	return socketSetBoolNative(socket, enabled, func(s *C.OmqGoSocket, enabled C.int) C.OmqGoStatus {
		return C.omq_go_socket_set_reconnect_stop_conn_refused(s, enabled)
	})
}

func setHeartbeatTTLNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_heartbeat_ttl((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setHeartbeatTimeoutNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_heartbeat_timeout((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setMaxPendingHandshakesNative(socket *nativeSocket, value int) error {
	if value <= 0 {
		return &ConfigError{Err: "max pending handshakes must be greater than zero"}
	}
	return statusErr(C.omq_go_socket_set_max_pending_handshakes((*C.OmqGoSocket)(socket), C.size_t(value)))
}

func setConflateNative(socket *nativeSocket, value bool) error {
	return socketSetBoolNative(socket, value, func(s *C.OmqGoSocket, enabled C.int) C.OmqGoStatus {
		return C.omq_go_socket_set_conflate(s, enabled)
	})
}

func setRouterMandatoryNative(socket *nativeSocket, value bool) error {
	return socketSetBoolNative(socket, value, func(s *C.OmqGoSocket, enabled C.int) C.OmqGoStatus {
		return C.omq_go_socket_set_router_mandatory(s, enabled)
	})
}

func setOnMuteNative(socket *nativeSocket, mode int32) error {
	return statusErr(C.omq_go_socket_set_on_mute((*C.OmqGoSocket)(socket), C.int32_t(mode)))
}

func setTCPKeepaliveNative(socket *nativeSocket, mode int32, idleMillis, intervalMillis int64, count uint32) error {
	return statusErr(C.omq_go_socket_set_tcp_keepalive(
		(*C.OmqGoSocket)(socket),
		C.int32_t(mode),
		C.int64_t(idleMillis),
		C.int64_t(intervalMillis),
		C.uint32_t(count),
	))
}

func setSendBufferSizeNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_send_buffer_size((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setRecvBufferSizeNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_recv_buffer_size((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setXPubNoDropNative(socket *nativeSocket, value bool) error {
	return socketSetBoolNative(socket, value, func(s *C.OmqGoSocket, enabled C.int) C.OmqGoStatus {
		return C.omq_go_socket_set_xpub_nodrop(s, enabled)
	})
}

func setCompressionAutoTrainNative(socket *nativeSocket, value bool) error {
	return socketSetBoolNative(socket, value, func(s *C.OmqGoSocket, enabled C.int) C.OmqGoStatus {
		return C.omq_go_socket_set_compression_auto_train(s, enabled)
	})
}

func setCompressionThresholdNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_compression_threshold((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setCompressionLevelNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_compression_level((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setCompressionDictNative(socket *nativeSocket, value []byte) error {
	return socketSetBytesNative(socket, value, func(s *C.OmqGoSocket, p *C.uint8_t, n C.size_t) C.OmqGoStatus {
		return C.omq_go_socket_set_compression_dict(s, p, n)
	})
}

func setCompressionDictCapacityNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_compression_dict_capacity((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setMaxRecvDictSizeNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_max_recv_dict_size((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setCompressionOffloadThresholdNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_compression_offload_threshold((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setLargeMessageThresholdNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_large_message_threshold((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setArenaThresholdNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_arena_threshold((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func setTransmitSlotCapacityNative(socket *nativeSocket, value int64) error {
	return statusErr(C.omq_go_socket_set_transmit_slot_cap((*C.OmqGoSocket)(socket), C.int64_t(value)))
}

func socketSetBoolNative(socket *nativeSocket, value bool, op func(*C.OmqGoSocket, C.int) C.OmqGoStatus) error {
	enabled := C.int(0)
	if value {
		enabled = 1
	}
	return statusErr(op((*C.OmqGoSocket)(socket), enabled))
}

func socketSetBytesNative(socket *nativeSocket, value []byte, op func(*C.OmqGoSocket, *C.uint8_t, C.size_t) C.OmqGoStatus) error {
	if len(value) == 0 {
		return statusErr(op((*C.OmqGoSocket)(socket), nil, 0))
	}
	ptr := C.CBytes(value)
	defer C.free(ptr)
	return statusErr(op((*C.OmqGoSocket)(socket), (*C.uint8_t)(ptr), C.size_t(len(value))))
}

func monitorNewNative(socket *nativeSocket) (*nativeMonitor, error) {
	var out *C.OmqGoMonitor
	err := statusErr(C.omq_go_socket_monitor((*C.OmqGoSocket)(socket), &out))
	if err != nil {
		return nil, err
	}
	return (*nativeMonitor)(out), nil
}

func monitorRecvNative(monitor *nativeMonitor) (MonitorEvent, error) {
	var out C.OmqGoEvent
	err := statusErr(C.omq_go_monitor_recv((*C.OmqGoMonitor)(monitor), 0, &out))
	if err != nil {
		return MonitorEvent{}, err
	}
	defer C.omq_go_event_free(out)
	return eventFromC(out), nil
}

func monitorCloseNative(monitor *nativeMonitor) {
	C.omq_go_monitor_close((*C.OmqGoMonitor)(monitor))
}

func monitorFreeNative(monitor *nativeMonitor) {
	C.omq_go_monitor_free((*C.OmqGoMonitor)(monitor))
}

func messageToC(msg Message) (*C.OmqGoPart, C.size_t, func()) {
	parts := msg.partsView()
	if len(parts) == 0 {
		return nil, 0, func() {}
	}
	size := C.size_t(len(parts)) * C.size_t(unsafe.Sizeof(C.OmqGoPart{}))
	ptr := C.malloc(size)
	arr := unsafe.Slice((*C.OmqGoPart)(ptr), len(parts))
	for i, part := range parts {
		if len(part) == 0 {
			arr[i].data = nil
			arr[i].len = 0
			continue
		}
		data := C.CBytes(part)
		arr[i].data = (*C.uint8_t)(data)
		arr[i].len = C.size_t(len(part))
	}
	free := func() {
		for _, part := range arr {
			if part.data != nil {
				C.free(unsafe.Pointer(part.data))
			}
		}
		C.free(ptr)
	}
	return (*C.OmqGoPart)(ptr), C.size_t(len(parts)), free
}

func messageFromC(raw C.OmqGoMessage) Message {
	if raw.parts == nil || raw.part_count == 0 {
		return Message{routingID: uint32(raw.routing_id)}
	}
	parts := unsafe.Slice(raw.parts, int(raw.part_count))
	out := make([][]byte, len(parts))
	for i, part := range parts {
		if part.data == nil || part.len == 0 {
			out[i] = []byte{}
			continue
		}
		out[i] = copyFromCPtr(part.data, part.len)
	}
	return Message{parts: out, routingID: uint32(raw.routing_id)}
}

func eventFromC(raw C.OmqGoEvent) MonitorEvent {
	ev := MonitorEvent{
		Kind:         goString(raw.kind),
		Endpoint:     goString(raw.endpoint),
		PeerIdent:    goString(raw.peer_ident),
		HasPeer:      raw.has_peer != 0,
		Reason:       goString(raw.reason),
		CommandName:  goString(raw.command_name),
		ConnectionID: uint64(raw.connection_id),
		Retry:        time.Duration(uint64(raw.retry_millis)) * time.Millisecond,
		Attempt:      uint32(raw.attempt),
	}
	if ev.HasPeer {
		ev.Peer = PeerInfo{
			Identity:     copyFromCPtr(raw.peer_identity, raw.peer_identity_len),
			ConnectionID: uint64(raw.connection_id),
			PeerAddress:  goString(raw.peer_address),
			SocketType:   goString(raw.peer_socket_type),
			ZMTPMajor:    uint8(raw.zmtp_major),
			ZMTPMinor:    uint8(raw.zmtp_minor),
		}
	}
	if raw.data != nil && raw.data_len > 0 {
		ev.Data = copyFromCPtr(raw.data, raw.data_len)
	}
	return ev
}

func peerInfoFromC(raw *C.OmqGoAuthPeer) PeerInfo {
	return PeerInfo{
		Mechanism: stringFromCBytes(raw.mechanism_data, raw.mechanism_len),
		PublicKey: stringFromCBytes(raw.public_key_data, raw.public_key_len),
		Identity:  bytesFromCBytes(raw.identity_data, raw.identity_len),
		PeerAddress: stringFromCBytes(
			raw.peer_address_data,
			raw.peer_address_len,
		),
		Username: stringFromCBytes(raw.username_data, raw.username_len),
		Password: stringFromCBytes(raw.password_data, raw.password_len),
	}
}

func stringFromCBytes(data *C.uint8_t, length C.size_t) string {
	if data == nil || length == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(unsafe.Pointer(data)), int(length)))
}

func bytesFromCBytes(data *C.uint8_t, length C.size_t) []byte {
	if data == nil || length == 0 {
		return nil
	}
	return append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(data)), int(length))...)
}

func copyFromCPtr(data *C.uint8_t, length C.size_t) []byte {
	n := int(length)
	if C.size_t(n) != length {
		panic("omq: native message exceeds Go slice capacity")
	}
	out := make([]byte, n)
	copy(out, unsafe.Slice((*byte)(unsafe.Pointer(data)), n))
	return out
}

func goString(value *C.char) string {
	if value == nil {
		return ""
	}
	return C.GoString(value)
}

func durationMillis(value time.Duration) int64 {
	if value < 0 {
		return -1
	}
	millis := value / time.Millisecond
	if value%time.Millisecond != 0 {
		millis++
	}
	return int64(millis)
}

func retryDelay(iteration int) time.Duration {
	if iteration < 10 {
		return 50 * time.Microsecond
	}
	if iteration < 100 {
		return 250 * time.Microsecond
	}
	return time.Millisecond
}

func waitRetry(ctx context.Context, iteration int) error {
	if iteration < 8 {
		return nil
	}
	if iteration < 256 {
		runtime.Gosched()
		return nil
	}
	timer := time.NewTimer(retryDelay(iteration))
	select {
	case <-ctx.Done():
		timer.Stop()
		return errFromContext(ctx)
	case <-timer.C:
		return nil
	}
}

func errFromContext(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout
	}
	return ErrCanceled
}

func keepAlive(values ...any) {
	for _, value := range values {
		runtime.KeepAlive(value)
	}
}
