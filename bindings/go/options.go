package omq

import "time"

type SocketOption func(*Socket) error

func SendHWM(value uint32) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setSendHWMNative(handle, value)
		})
		return err
	}
}

func RecvHWM(value uint32) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setRecvHWMNative(handle, value)
		})
		return err
	}
}

func Linger(value time.Duration) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setLingerNative(handle, durationMillis(value))
		})
		return err
	}
}

func Identity(value []byte) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setIdentityNative(handle, value)
		})
		return err
	}
}

func Conflate(enabled bool) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setConflateNative(handle, enabled)
		})
		return err
	}
}

func RouterMandatory(enabled bool) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setRouterMandatoryNative(handle, enabled)
		})
		return err
	}
}

func XPubNoDrop(enabled bool) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setXPubNoDropNative(handle, enabled)
		})
		return err
	}
}

func CompressionAutoTrain(enabled bool) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setCompressionAutoTrainNative(handle, enabled)
		})
		return err
	}
}

func CompressionThreshold(bytes int) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setCompressionThresholdNative(handle, int64(bytes))
		})
		return err
	}
}

func CompressionLevel(level int) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setCompressionLevelNative(handle, int64(level))
		})
		return err
	}
}

func CompressionDict(value []byte) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, setCompressionDictNative(handle, value)
		})
		return err
	}
}
