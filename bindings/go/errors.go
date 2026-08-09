package omq

import "errors"

var (
	ErrAgain      = errors.New("omq: operation would block")
	ErrClosed     = errors.New("omq: closed")
	ErrTimeout    = errors.New("omq: operation timed out")
	ErrCanceled   = errors.New("omq: operation canceled")
	ErrUnroutable = errors.New("omq: no route to peer")
)

type Error struct {
	Err string
}

func (e *Error) Error() string {
	return "omq: " + e.Err
}

type EndpointError struct {
	Op  string
	Err string
}

func (e *EndpointError) Error() string {
	return "omq: " + e.Op + ": " + e.Err
}

type ProtocolError struct {
	Err string
}

func (e *ProtocolError) Error() string {
	return "omq: protocol: " + e.Err
}

type ConfigError struct {
	Err string
}

func (e *ConfigError) Error() string {
	return "omq: config: " + e.Err
}

type TransportError struct {
	Err string
}

func (e *TransportError) Error() string {
	return "omq: transport: " + e.Err
}

type MessageTooLargeError struct {
	Err string
}

func (e *MessageTooLargeError) Error() string {
	return "omq: message too large: " + e.Err
}
