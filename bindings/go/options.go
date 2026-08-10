package omq

import "time"

type SocketOption func(*Socket) error

const defaultCompressionLevel = int64(-1 << 63)
const (
	zmtpMaxShortStringBytes = 255
	compressionDictMaxBytes = 8 * 1024
	zstdLevelMin            = -8
	zstdLevelMax            = 4
	maxHeartbeatTTLMillis   = 6_553_500
)

func nativeOption(op func(*nativeSocket) error) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, op(handle)
		})
		return err
	}
}

func SendHWM(value uint32) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setSendHWMNative(handle, value)
	})
}

func RecvHWM(value uint32) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setRecvHWMNative(handle, value)
	})
}

func Linger(value time.Duration) SocketOption {
	return durationOption(value, setLingerNative)
}

func LingerForever() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setLingerNative(handle, -1)
	})
}

func Identity(value []byte) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if len(value) > zmtpMaxShortStringBytes {
			return &ConfigError{Err: "identity length must be at most 255 bytes"}
		}
		return setIdentityNative(handle, value)
	})
}

func HeartbeatInterval(value time.Duration) SocketOption {
	return durationOption(value, setHeartbeatIntervalNative)
}

func HeartbeatOff() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setHeartbeatIntervalNative(handle, -1)
	})
}

func HeartbeatTTL(value time.Duration) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		millis, err := nonNegativeMillis("heartbeat TTL", value)
		if err != nil {
			return err
		}
		if millis > maxHeartbeatTTLMillis {
			return &ConfigError{Err: "heartbeat TTL exceeds ZMTP maximum of 6553.5s"}
		}
		return setHeartbeatTTLNative(handle, millis)
	})
}

func NoHeartbeatTTL() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setHeartbeatTTLNative(handle, -1)
	})
}

func HeartbeatTimeout(value time.Duration) SocketOption {
	return durationOption(value, setHeartbeatTimeoutNative)
}

func DefaultHeartbeatTimeout() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setHeartbeatTimeoutNative(handle, -1)
	})
}

func HandshakeTimeout(value time.Duration) SocketOption {
	return durationOption(value, setHandshakeTimeoutNative)
}

func NoHandshakeTimeout() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setHandshakeTimeoutNative(handle, -1)
	})
}

func MaxMessageSize(bytes int64) SocketOption {
	return nonNegativeInt64Option("max message size", bytes, setMaxMessageSizeNative)
}

func NoMaxMessageSize() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setMaxMessageSizeNative(handle, -1)
	})
}

func PlainServer(username, password string) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if err := validateZmtpShortString("PLAIN username", username); err != nil {
			return err
		}
		if err := validateZmtpShortString("PLAIN password", password); err != nil {
			return err
		}
		return setPlainServerNative(handle, username, password)
	})
}

func PlainServerAuth(auth Authenticator) SocketOption {
	return authOption(auth, func(id uint64) SocketOption {
		return nativeOption(func(handle *nativeSocket) error {
			return setPlainServerAuthNative(handle, id)
		})
	})
}

func PlainClient(username, password string) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if err := validateZmtpShortString("PLAIN username", username); err != nil {
			return err
		}
		if err := validateZmtpShortString("PLAIN password", password); err != nil {
			return err
		}
		return setPlainClientNative(handle, username, password)
	})
}

func CurveServer(keypair CurveKeypair) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if err := validateCurveKeypair(keypair); err != nil {
			return err
		}
		return setCurveServerNative(handle, keypair)
	})
}

func CurveServerAuth(keypair CurveKeypair, auth Authenticator) SocketOption {
	return authOption(auth, func(id uint64) SocketOption {
		return nativeOption(func(handle *nativeSocket) error {
			if err := validateCurveKeypair(keypair); err != nil {
				return err
			}
			return setCurveServerAuthNative(handle, keypair, id)
		})
	})
}

func CurveClient(keypair CurveKeypair, serverPublicKey string) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if err := validateCurveKeypair(keypair); err != nil {
			return err
		}
		return setCurveClientNative(handle, keypair, serverPublicKey)
	})
}

func Workload(profile WorkloadProfile) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setWorkloadProfileNative(handle, int32(profile))
	})
}

func DefaultWorkload() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setWorkloadProfileNative(handle, -1)
	})
}

func ReconnectDisabled() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setReconnectNative(handle, 0, 0, 0)
	})
}

func ReconnectInterval(value time.Duration) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		millis, err := nonNegativeMillis("reconnect interval", value)
		if err != nil {
			return err
		}
		return setReconnectNative(handle, 1, millis, 0)
	})
}

func ReconnectExponential(min, max time.Duration) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		minMillis, err := nonNegativeMillis("reconnect min", min)
		if err != nil {
			return err
		}
		maxMillis, err := nonNegativeMillis("reconnect max", max)
		if err != nil {
			return err
		}
		if maxMillis < minMillis {
			return &ConfigError{Err: "reconnect max must be greater than or equal to min"}
		}
		return setReconnectNative(handle, 2, minMillis, maxMillis)
	})
}

func ReconnectStopConnRefused(enabled bool) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setReconnectStopConnRefusedNative(handle, enabled)
	})
}

func MaxPendingHandshakes(value int) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setMaxPendingHandshakesNative(handle, value)
	})
}

func Conflate(enabled bool) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setConflateNative(handle, enabled)
	})
}

func RouterMandatory(enabled bool) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setRouterMandatoryNative(handle, enabled)
	})
}

func OnMutePolicy(mode OnMute) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setOnMuteNative(handle, int32(mode))
	})
}

func TCPKeepaliveDefault() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setTCPKeepaliveNative(handle, 0, 0, 0, 0)
	})
}

func TCPKeepaliveOff() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setTCPKeepaliveNative(handle, 1, 0, 0, 0)
	})
}

func TCPKeepalive(idle, interval time.Duration, count uint32) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if count == 0 {
			return &ConfigError{Err: "TCP keepalive count must be greater than zero"}
		}
		idleMillis, err := nonNegativeMillis("TCP keepalive idle", idle)
		if err != nil {
			return err
		}
		intervalMillis, err := nonNegativeMillis("TCP keepalive interval", interval)
		if err != nil {
			return err
		}
		return setTCPKeepaliveNative(handle, 2, idleMillis, intervalMillis, count)
	})
}

func SendBufferSize(bytes int64) SocketOption {
	return nonNegativeInt64Option("send buffer size", bytes, setSendBufferSizeNative)
}

func DefaultSendBufferSize() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setSendBufferSizeNative(handle, -1)
	})
}

func RecvBufferSize(bytes int64) SocketOption {
	return nonNegativeInt64Option("receive buffer size", bytes, setRecvBufferSizeNative)
}

func DefaultRecvBufferSize() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setRecvBufferSizeNative(handle, -1)
	})
}

func XPubNoDrop(enabled bool) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setXPubNoDropNative(handle, enabled)
	})
}

func CompressionAutoTrain(enabled bool) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setCompressionAutoTrainNative(handle, enabled)
	})
}

func CompressionThreshold(bytes int) SocketOption {
	return nonNegativeInt64Option("compression threshold", int64(bytes), setCompressionThresholdNative)
}

func CompressionDefaultThreshold() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setCompressionThresholdNative(handle, -1)
	})
}

func CompressionLevel(level int) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if level < zstdLevelMin || level > zstdLevelMax {
			return &ConfigError{Err: "zstd compression level must be -8..=4"}
		}
		return setCompressionLevelNative(handle, int64(level))
	})
}

func CompressionDefaultLevel() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setCompressionLevelNative(handle, defaultCompressionLevel)
	})
}

func CompressionDict(value []byte) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if len(value) == 0 {
			return &ConfigError{Err: "compression dict must not be empty"}
		}
		if len(value) > compressionDictMaxBytes {
			return &ConfigError{Err: "compression dict length must be at most 8192 bytes"}
		}
		return setCompressionDictNative(handle, value)
	})
}

func NoCompressionDict() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setCompressionDictNative(handle, nil)
	})
}

func CompressionDictCapacity(bytes int64) SocketOption {
	return nonNegativeInt64Option("compression dictionary capacity", bytes, setCompressionDictCapacityNative)
}

func DefaultCompressionDictCapacity() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setCompressionDictCapacityNative(handle, -1)
	})
}

func MaxRecvDictSize(bytes int64) SocketOption {
	return nonNegativeInt64Option("max receive dictionary size", bytes, setMaxRecvDictSizeNative)
}

func DefaultMaxRecvDictSize() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setMaxRecvDictSizeNative(handle, -1)
	})
}

func CompressionOffloadThreshold(bytes int64) SocketOption {
	return nonNegativeInt64Option("compression offload threshold", bytes, setCompressionOffloadThresholdNative)
}

func NoCompressionOffload() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setCompressionOffloadThresholdNative(handle, -1)
	})
}

func LargeMessageThreshold(bytes int64) SocketOption {
	return nonNegativeInt64Option("large message threshold", bytes, setLargeMessageThresholdNative)
}

func DisableLargeMessagePath() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setLargeMessageThresholdNative(handle, -1)
	})
}

func ArenaThreshold(bytes int64) SocketOption {
	return nonNegativeInt64Option("arena threshold", bytes, setArenaThresholdNative)
}

func DefaultArenaThreshold() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setArenaThresholdNative(handle, -1)
	})
}

func TransmitSlotCapacity(bytes int64) SocketOption {
	return nonNegativeInt64Option("transmit slot capacity", bytes, setTransmitSlotCapacityNative)
}

func DefaultTransmitSlotCapacity() SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		return setTransmitSlotCapacityNative(handle, -1)
	})
}

func durationOption(value time.Duration, set func(*nativeSocket, int64) error) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		millis, err := nonNegativeMillis("duration", value)
		if err != nil {
			return err
		}
		return set(handle, millis)
	})
}

func nonNegativeMillis(name string, value time.Duration) (int64, error) {
	if value < 0 {
		return 0, &ConfigError{Err: name + " must be non-negative"}
	}
	return durationMillis(value), nil
}

func nonNegativeInt64Option(name string, value int64, set func(*nativeSocket, int64) error) SocketOption {
	return nativeOption(func(handle *nativeSocket) error {
		if value < 0 {
			return &ConfigError{Err: name + " must be non-negative"}
		}
		return set(handle, value)
	})
}

func validateZmtpShortString(name, value string) error {
	if len([]byte(value)) > zmtpMaxShortStringBytes {
		return &ConfigError{Err: name + " length must be at most 255 bytes"}
	}
	return nil
}

func authOption(auth Authenticator, option func(uint64) SocketOption) SocketOption {
	return func(s *Socket) error {
		if auth == nil {
			return &ConfigError{Err: "authenticator must not be nil"}
		}
		if s == nil {
			return ErrClosed
		}
		id := registerAuthCallback(auth)
		if err := option(id)(s); err != nil {
			unregisterAuthCallback(id)
			return err
		}
		s.addAuthCallback(id)
		return nil
	}
}

func validateCurveKeypair(keypair CurveKeypair) error {
	public, err := CurvePublic(keypair.Secret)
	if err != nil {
		return err
	}
	if public != keypair.Public {
		return &ConfigError{Err: "CURVE public key does not match secret key"}
	}
	return nil
}
