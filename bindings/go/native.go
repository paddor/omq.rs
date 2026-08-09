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
	"time"
	"unsafe"
)

type nativeContext = C.OmqGoContext
type nativeSocket = C.OmqGoSocket
type nativeMonitor = C.OmqGoMonitor

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

func socketConnectNative(socket *nativeSocket, endpoint string) error {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	return statusErr(C.omq_go_socket_connect((*C.OmqGoSocket)(socket), cEndpoint))
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
	parts, count, free := messageToC(msg)
	defer free()
	return statusErr(C.omq_go_socket_send((*C.OmqGoSocket)(socket), parts, count, 0))
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

func socketMessageRecvNative(socket *nativeSocket) (Message, error) {
	var out C.OmqGoMessage
	err := statusErr(C.omq_go_socket_recv((*C.OmqGoSocket)(socket), 0, &out))
	if err != nil {
		return Message{}, err
	}
	defer C.omq_go_message_free(out)
	return messageFromC(out), nil
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
	parts := msg.Parts()
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
		return Message{}
	}
	parts := unsafe.Slice(raw.parts, int(raw.part_count))
	out := make([][]byte, len(parts))
	for i, part := range parts {
		if part.data == nil || part.len == 0 {
			out[i] = []byte{}
			continue
		}
		out[i] = C.GoBytes(unsafe.Pointer(part.data), C.int(part.len))
	}
	return NewMessage(out...)
}

func eventFromC(raw C.OmqGoEvent) MonitorEvent {
	ev := MonitorEvent{
		Kind:         goString(raw.kind),
		Endpoint:     goString(raw.endpoint),
		PeerIdent:    goString(raw.peer_ident),
		Reason:       goString(raw.reason),
		CommandName:  goString(raw.command_name),
		ConnectionID: uint64(raw.connection_id),
		Retry:        time.Duration(uint64(raw.retry_millis)) * time.Millisecond,
		Attempt:      uint32(raw.attempt),
	}
	if raw.data != nil && raw.data_len > 0 {
		ev.Data = C.GoBytes(unsafe.Pointer(raw.data), C.int(raw.data_len))
	}
	return ev
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
	return int64(value / time.Millisecond)
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
