package omq

import "errors"

// ErrAgain reports that a nonblocking operation would block.
var (
	ErrAgain      = errors.New("omq: operation would block")
	ErrClosed     = errors.New("omq: closed")
	ErrTimeout    = errors.New("omq: operation timed out")
	ErrCanceled   = errors.New("omq: operation canceled")
	ErrUnroutable = errors.New("omq: no route to peer")
)

// Error is a generic OMQ error.
type Error struct {
	// Err is the native error detail.
	Err string
}

func (e *Error) Error() string {
	return "omq: " + e.Err
}

// EndpointError reports endpoint parsing or scheme errors.
type EndpointError struct {
	// Op is the endpoint operation.
	Op string
	// Err is the endpoint error detail.
	Err string
}

func (e *EndpointError) Error() string {
	return "omq: " + e.Op + ": " + e.Err
}

// ProtocolError reports socket type or ZMTP protocol violations.
type ProtocolError struct {
	// Err is the protocol error detail.
	Err string
}

func (e *ProtocolError) Error() string {
	return "omq: protocol: " + e.Err
}

// ConfigError reports invalid configuration.
type ConfigError struct {
	// Err is the config error detail.
	Err string
}

func (e *ConfigError) Error() string {
	return "omq: config: " + e.Err
}

// TransportError reports transport I/O errors.
type TransportError struct {
	// Err is the transport error detail.
	Err string
}

func (e *TransportError) Error() string {
	return "omq: transport: " + e.Err
}

// MessageTooLargeError reports a message size limit violation.
type MessageTooLargeError struct {
	// Err is the message size error detail.
	Err string
}

func (e *MessageTooLargeError) Error() string {
	return "omq: message too large: " + e.Err
}
