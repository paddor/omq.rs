package omq

import "time"

// SocketOption configures a socket before its first bind, connect, or I/O.
type SocketOption func(*Socket) error

// OptionValue stores an optional configured value in a socket option snapshot.
type OptionValue[T any] struct {
	// Value is the configured value.
	Value T
	// Set reports whether Value is configured.
	Set bool
}

// SocketOptions reports options configured through OMQ.go.
type SocketOptions struct {
	// SendHWM is the configured outbound high-water mark.
	SendHWM OptionValue[uint32]
	// RecvHWM is the configured inbound high-water mark.
	RecvHWM OptionValue[uint32]
	// Linger is the configured close linger.
	Linger OptionValue[time.Duration]
	// LingerNever reports linger-forever configuration.
	LingerNever bool
	// Identity is the configured ZMTP identity.
	Identity OptionValue[[]byte]

	// HeartbeatInterval is the configured heartbeat interval.
	HeartbeatInterval OptionValue[time.Duration]
	// HeartbeatOff reports disabled heartbeats.
	HeartbeatOff bool
	// HeartbeatTTL is the configured heartbeat TTL.
	HeartbeatTTL OptionValue[time.Duration]
	// NoHeartbeatTTL reports disabled heartbeat TTL.
	NoHeartbeatTTL bool
	// HeartbeatTimeout is the configured heartbeat timeout.
	HeartbeatTimeout OptionValue[time.Duration]
	// DefaultHeartbeatTimeout reports default heartbeat timeout.
	DefaultHeartbeatTimeout bool
	// HandshakeTimeout is the configured handshake timeout.
	HandshakeTimeout OptionValue[time.Duration]
	// NoHandshakeTimeout reports disabled handshake timeout.
	NoHandshakeTimeout bool
	// MaxMessageSize is the configured receive size limit.
	MaxMessageSize OptionValue[int64]
	// NoMaxMessageSize reports disabled receive size limit.
	NoMaxMessageSize bool

	// PlainServer is fixed PLAIN server credentials.
	PlainServer OptionValue[PlainCredentials]
	// PlainServerAuth reports a PLAIN server callback.
	PlainServerAuth bool
	// PlainClient is PLAIN client credentials.
	PlainClient OptionValue[PlainCredentials]
	// CurveServer is CURVE server key material.
	CurveServer OptionValue[CurveKeypair]
	// CurveServerAuth reports a CURVE server callback.
	CurveServerAuth bool
	// CurveClient is CURVE client key material.
	CurveClient OptionValue[CurveClientConfig]

	// Workload is native workload tuning.
	Workload OptionValue[WorkloadProfile]
	// DefaultWorkload reports default workload tuning.
	DefaultWorkload bool
	// Reconnect is configured reconnect behavior.
	Reconnect OptionValue[ReconnectConfig]
	// ReconnectStopConnRefused is stop-on-refused behavior.
	ReconnectStopConnRefused OptionValue[bool]
	// MaxPendingHandshakes is the configured handshake cap.
	MaxPendingHandshakes OptionValue[int]
	// Conflate is configured conflate behavior.
	Conflate OptionValue[bool]
	// RouterMandatory is configured ROUTER mandatory behavior.
	RouterMandatory OptionValue[bool]
	// OnMute is configured mute behavior.
	OnMute OptionValue[OnMute]
	// TCPKeepalive is configured TCP keepalive behavior.
	TCPKeepalive OptionValue[TCPKeepaliveConfig]
	// SendBufferSize is configured OS send buffer size.
	SendBufferSize OptionValue[int64]
	// DefaultSendBufferSize reports default OS send buffer size.
	DefaultSendBufferSize bool
	// RecvBufferSize is configured OS receive buffer size.
	RecvBufferSize OptionValue[int64]
	// DefaultRecvBufferSize reports default OS receive buffer size.
	DefaultRecvBufferSize bool
	// XPubNoDrop is configured XPUB no-drop behavior.
	XPubNoDrop OptionValue[bool]

	// CompressionAutoTrain is configured dictionary auto-training.
	CompressionAutoTrain OptionValue[bool]
	// CompressionThreshold is configured compression threshold.
	CompressionThreshold OptionValue[int64]
	// CompressionDefaultThreshold reports default compression threshold.
	CompressionDefaultThreshold bool
	// CompressionLevel is configured zstd compression level.
	CompressionLevel OptionValue[int]
	// CompressionDefaultLevel reports default compression level.
	CompressionDefaultLevel bool
	// CompressionDict is the static compression dictionary.
	CompressionDict OptionValue[[]byte]
	// NoCompressionDict reports cleared static compression dictionary.
	NoCompressionDict bool
	// CompressionDictCapacity is configured dictionary capacity.
	CompressionDictCapacity OptionValue[int64]
	// DefaultCompressionDictCapacity reports default dictionary capacity.
	DefaultCompressionDictCapacity bool
	// MaxRecvDictSize is configured max received dictionary size.
	MaxRecvDictSize OptionValue[int64]
	// DefaultMaxRecvDictSize reports default max received dictionary size.
	DefaultMaxRecvDictSize bool
	// CompressionOffloadThreshold is configured compression offload threshold.
	CompressionOffloadThreshold OptionValue[int64]
	// NoCompressionOffload reports disabled compression offload.
	NoCompressionOffload bool
	// LargeMessageThreshold is configured large-message threshold.
	LargeMessageThreshold OptionValue[int64]
	// DisableLargeMessagePath reports disabled large-message path.
	DisableLargeMessagePath bool
	// ArenaThreshold is configured frame arena threshold.
	ArenaThreshold OptionValue[int64]
	// DefaultArenaThreshold reports default arena threshold.
	DefaultArenaThreshold bool
	// TransmitSlotCapacity is configured transmit slot capacity.
	TransmitSlotCapacity OptionValue[int64]
	// DefaultTransmitSlotCapacity reports default transmit slot capacity.
	DefaultTransmitSlotCapacity bool
}

// PlainCredentials stores configured PLAIN username and password.
type PlainCredentials struct {
	// Username is the PLAIN username.
	Username string
	// Password is the PLAIN password.
	Password string
}

// CurveClientConfig stores configured CURVE client keys.
type CurveClientConfig struct {
	// Keypair is the client keypair.
	Keypair CurveKeypair
	// ServerPublicKey is the expected server public key.
	ServerPublicKey string
}

// ReconnectConfig stores configured reconnect behavior.
type ReconnectConfig struct {
	// Mode is disabled, fixed, or exponential.
	Mode string
	// Min is the fixed interval or exponential lower bound.
	Min time.Duration
	// Max is the exponential upper bound.
	Max time.Duration
}

// TCPKeepaliveConfig stores configured TCP keepalive behavior.
type TCPKeepaliveConfig struct {
	// Mode is default, off, or enabled.
	Mode string
	// Idle is TCP keepalive idle time.
	Idle time.Duration
	// Interval is TCP keepalive probe interval.
	Interval time.Duration
	// Count is TCP keepalive probe count.
	Count uint32
}

const defaultCompressionLevel = int64(-1 << 63)
const (
	zmtpMaxShortStringBytes = 255
	compressionDictMaxBytes = 8 * 1024
	zstdLevelMin            = -8
	zstdLevelMax            = 4
	maxHeartbeatTTLMillis   = 6_553_500
)

func nativeOption(op func(*nativeSocket) error) SocketOption {
	return trackedOption(op, nil)
}

func trackedOption(op func(*nativeSocket) error, record func(*SocketOptions)) SocketOption {
	return func(s *Socket) error {
		_, err := s.call(nil, false, func(handle *nativeSocket) (any, error) {
			return nil, op(handle)
		})
		if err == nil {
			s.recordOption(record)
		}
		return err
	}
}

// SendHWM sets the outbound high-water mark.
func SendHWM(value uint32) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setSendHWMNative(handle, value)
	}, func(options *SocketOptions) {
		options.SendHWM = OptionValue[uint32]{Value: value, Set: true}
	})
}

// RecvHWM sets the inbound high-water mark.
func RecvHWM(value uint32) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setRecvHWMNative(handle, value)
	}, func(options *SocketOptions) {
		options.RecvHWM = OptionValue[uint32]{Value: value, Set: true}
	})
}

// Linger sets socket close linger.
func Linger(value time.Duration) SocketOption {
	return trackedDurationOption(value, setLingerNative, func(options *SocketOptions, value time.Duration) {
		options.Linger = OptionValue[time.Duration]{Value: value, Set: true}
		options.LingerNever = false
	})
}

// LingerForever keeps pending messages until delivered on close.
func LingerForever() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setLingerNative(handle, -1)
	}, func(options *SocketOptions) {
		options.Linger = OptionValue[time.Duration]{}
		options.LingerNever = true
	})
}

// Identity sets the ZMTP routing identity.
func Identity(value []byte) SocketOption {
	copied := append([]byte(nil), value...)
	return trackedOption(func(handle *nativeSocket) error {
		if len(value) > zmtpMaxShortStringBytes {
			return &ConfigError{Err: "identity length must be at most 255 bytes"}
		}
		return setIdentityNative(handle, value)
	}, func(options *SocketOptions) {
		options.Identity = OptionValue[[]byte]{Value: append([]byte(nil), copied...), Set: true}
	})
}

// HeartbeatInterval sets ZMTP heartbeat interval.
func HeartbeatInterval(value time.Duration) SocketOption {
	return trackedDurationOption(value, setHeartbeatIntervalNative, func(options *SocketOptions, value time.Duration) {
		options.HeartbeatInterval = OptionValue[time.Duration]{Value: value, Set: true}
		options.HeartbeatOff = false
	})
}

// HeartbeatOff disables heartbeats.
func HeartbeatOff() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setHeartbeatIntervalNative(handle, -1)
	}, func(options *SocketOptions) {
		options.HeartbeatInterval = OptionValue[time.Duration]{}
		options.HeartbeatOff = true
	})
}

// HeartbeatTTL sets ZMTP heartbeat TTL.
func HeartbeatTTL(value time.Duration) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		millis, err := nonNegativeMillis("heartbeat TTL", value)
		if err != nil {
			return err
		}
		if millis > maxHeartbeatTTLMillis {
			return &ConfigError{Err: "heartbeat TTL exceeds ZMTP maximum of 6553.5s"}
		}
		return setHeartbeatTTLNative(handle, millis)
	}, func(options *SocketOptions) {
		options.HeartbeatTTL = OptionValue[time.Duration]{Value: value, Set: true}
		options.NoHeartbeatTTL = false
	})
}

// NoHeartbeatTTL disables heartbeat TTL.
func NoHeartbeatTTL() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setHeartbeatTTLNative(handle, -1)
	}, func(options *SocketOptions) {
		options.HeartbeatTTL = OptionValue[time.Duration]{}
		options.NoHeartbeatTTL = true
	})
}

// HeartbeatTimeout sets missing-heartbeat timeout.
func HeartbeatTimeout(value time.Duration) SocketOption {
	return trackedDurationOption(value, setHeartbeatTimeoutNative, func(options *SocketOptions, value time.Duration) {
		options.HeartbeatTimeout = OptionValue[time.Duration]{Value: value, Set: true}
		options.DefaultHeartbeatTimeout = false
	})
}

// DefaultHeartbeatTimeout restores native heartbeat timeout default.
func DefaultHeartbeatTimeout() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setHeartbeatTimeoutNative(handle, -1)
	}, func(options *SocketOptions) {
		options.HeartbeatTimeout = OptionValue[time.Duration]{}
		options.DefaultHeartbeatTimeout = true
	})
}

// HandshakeTimeout sets ZMTP handshake timeout.
func HandshakeTimeout(value time.Duration) SocketOption {
	return trackedDurationOption(value, setHandshakeTimeoutNative, func(options *SocketOptions, value time.Duration) {
		options.HandshakeTimeout = OptionValue[time.Duration]{Value: value, Set: true}
		options.NoHandshakeTimeout = false
	})
}

// NoHandshakeTimeout disables handshake timeout.
func NoHandshakeTimeout() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setHandshakeTimeoutNative(handle, -1)
	}, func(options *SocketOptions) {
		options.HandshakeTimeout = OptionValue[time.Duration]{}
		options.NoHandshakeTimeout = true
	})
}

// MaxMessageSize sets max receive message size in bytes.
func MaxMessageSize(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("max message size", bytes, setMaxMessageSizeNative, func(options *SocketOptions, value int64) {
		options.MaxMessageSize = OptionValue[int64]{Value: value, Set: true}
		options.NoMaxMessageSize = false
	})
}

// NoMaxMessageSize removes max receive message size.
func NoMaxMessageSize() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setMaxMessageSizeNative(handle, -1)
	}, func(options *SocketOptions) {
		options.MaxMessageSize = OptionValue[int64]{}
		options.NoMaxMessageSize = true
	})
}

// PlainServer configures fixed PLAIN server credentials.
func PlainServer(username, password string) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		if err := validateZmtpShortString("PLAIN username", username); err != nil {
			return err
		}
		if err := validateZmtpShortString("PLAIN password", password); err != nil {
			return err
		}
		return setPlainServerNative(handle, username, password)
	}, func(options *SocketOptions) {
		options.PlainServer = OptionValue[PlainCredentials]{
			Value: PlainCredentials{Username: username, Password: password},
			Set:   true,
		}
		options.PlainServerAuth = false
	})
}

// PlainServerAuth configures a PLAIN server authenticator.
func PlainServerAuth(auth Authenticator) SocketOption {
	return authOption(auth, func(id uint64) SocketOption {
		return nativeOption(func(handle *nativeSocket) error {
			return setPlainServerAuthNative(handle, id)
		})
	}, func(options *SocketOptions) {
		options.PlainServer = OptionValue[PlainCredentials]{}
		options.PlainServerAuth = true
	})
}

// PlainClient configures PLAIN client credentials.
func PlainClient(username, password string) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		if err := validateZmtpShortString("PLAIN username", username); err != nil {
			return err
		}
		if err := validateZmtpShortString("PLAIN password", password); err != nil {
			return err
		}
		return setPlainClientNative(handle, username, password)
	}, func(options *SocketOptions) {
		options.PlainClient = OptionValue[PlainCredentials]{
			Value: PlainCredentials{Username: username, Password: password},
			Set:   true,
		}
	})
}

// CurveServer configures a CURVE server.
func CurveServer(keypair CurveKeypair) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		if err := validateCurveKeypair(keypair); err != nil {
			return err
		}
		return setCurveServerNative(handle, keypair)
	}, func(options *SocketOptions) {
		options.CurveServer = OptionValue[CurveKeypair]{Value: keypair, Set: true}
		options.CurveServerAuth = false
	})
}

// CurveServerAuth configures a CURVE server authenticator.
func CurveServerAuth(keypair CurveKeypair, auth Authenticator) SocketOption {
	return authOption(auth, func(id uint64) SocketOption {
		return nativeOption(func(handle *nativeSocket) error {
			if err := validateCurveKeypair(keypair); err != nil {
				return err
			}
			return setCurveServerAuthNative(handle, keypair, id)
		})
	}, func(options *SocketOptions) {
		options.CurveServer = OptionValue[CurveKeypair]{Value: keypair, Set: true}
		options.CurveServerAuth = true
	})
}

// CurveClient configures a CURVE client.
func CurveClient(keypair CurveKeypair, serverPublicKey string) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		if err := validateCurveKeypair(keypair); err != nil {
			return err
		}
		return setCurveClientNative(handle, keypair, serverPublicKey)
	}, func(options *SocketOptions) {
		options.CurveClient = OptionValue[CurveClientConfig]{
			Value: CurveClientConfig{Keypair: keypair, ServerPublicKey: serverPublicKey},
			Set:   true,
		}
	})
}

// Workload sets native workload tuning.
func Workload(profile WorkloadProfile) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setWorkloadProfileNative(handle, int32(profile))
	}, func(options *SocketOptions) {
		options.Workload = OptionValue[WorkloadProfile]{Value: profile, Set: true}
		options.DefaultWorkload = false
	})
}

// DefaultWorkload restores native workload tuning.
func DefaultWorkload() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setWorkloadProfileNative(handle, -1)
	}, func(options *SocketOptions) {
		options.Workload = OptionValue[WorkloadProfile]{}
		options.DefaultWorkload = true
	})
}

// ReconnectDisabled disables automatic reconnect.
func ReconnectDisabled() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setReconnectNative(handle, 0, 0, 0)
	}, func(options *SocketOptions) {
		options.Reconnect = OptionValue[ReconnectConfig]{
			Value: ReconnectConfig{Mode: "disabled"},
			Set:   true,
		}
	})
}

// ReconnectInterval sets fixed reconnect interval.
func ReconnectInterval(value time.Duration) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		millis, err := nonNegativeMillis("reconnect interval", value)
		if err != nil {
			return err
		}
		return setReconnectNative(handle, 1, millis, 0)
	}, func(options *SocketOptions) {
		options.Reconnect = OptionValue[ReconnectConfig]{
			Value: ReconnectConfig{Mode: "fixed", Min: value},
			Set:   true,
		}
	})
}

// ReconnectExponential sets exponential reconnect bounds.
func ReconnectExponential(min, max time.Duration) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
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
	}, func(options *SocketOptions) {
		options.Reconnect = OptionValue[ReconnectConfig]{
			Value: ReconnectConfig{Mode: "exponential", Min: min, Max: max},
			Set:   true,
		}
	})
}

// ReconnectStopConnRefused toggles stop-on-ECONNREFUSED reconnect behavior.
func ReconnectStopConnRefused(enabled bool) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setReconnectStopConnRefusedNative(handle, enabled)
	}, func(options *SocketOptions) {
		options.ReconnectStopConnRefused = OptionValue[bool]{Value: enabled, Set: true}
	})
}

// MaxPendingHandshakes limits concurrent pending handshakes.
func MaxPendingHandshakes(value int) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setMaxPendingHandshakesNative(handle, value)
	}, func(options *SocketOptions) {
		options.MaxPendingHandshakes = OptionValue[int]{Value: value, Set: true}
	})
}

// Conflate keeps only the latest queued message.
func Conflate(enabled bool) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setConflateNative(handle, enabled)
	}, func(options *SocketOptions) {
		options.Conflate = OptionValue[bool]{Value: enabled, Set: true}
	})
}

// RouterMandatory toggles ROUTER unroutable send errors.
func RouterMandatory(enabled bool) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setRouterMandatoryNative(handle, enabled)
	}, func(options *SocketOptions) {
		options.RouterMandatory = OptionValue[bool]{Value: enabled, Set: true}
	})
}

// OnMutePolicy sets behavior when native outbound queues are full.
func OnMutePolicy(mode OnMute) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setOnMuteNative(handle, int32(mode))
	}, func(options *SocketOptions) {
		options.OnMute = OptionValue[OnMute]{Value: mode, Set: true}
	})
}

// TCPKeepaliveDefault restores platform TCP keepalive defaults.
func TCPKeepaliveDefault() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setTCPKeepaliveNative(handle, 0, 0, 0, 0)
	}, func(options *SocketOptions) {
		options.TCPKeepalive = OptionValue[TCPKeepaliveConfig]{
			Value: TCPKeepaliveConfig{Mode: "default"},
			Set:   true,
		}
	})
}

// TCPKeepaliveOff disables TCP keepalive.
func TCPKeepaliveOff() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setTCPKeepaliveNative(handle, 1, 0, 0, 0)
	}, func(options *SocketOptions) {
		options.TCPKeepalive = OptionValue[TCPKeepaliveConfig]{
			Value: TCPKeepaliveConfig{Mode: "off"},
			Set:   true,
		}
	})
}

// TCPKeepalive enables TCP keepalive with idle, interval, and probe count.
func TCPKeepalive(idle, interval time.Duration, count uint32) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
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
	}, func(options *SocketOptions) {
		options.TCPKeepalive = OptionValue[TCPKeepaliveConfig]{
			Value: TCPKeepaliveConfig{
				Mode:     "enabled",
				Idle:     idle,
				Interval: interval,
				Count:    count,
			},
			Set: true,
		}
	})
}

// SendBufferSize sets OS send buffer size.
func SendBufferSize(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("send buffer size", bytes, setSendBufferSizeNative, func(options *SocketOptions, value int64) {
		options.SendBufferSize = OptionValue[int64]{Value: value, Set: true}
		options.DefaultSendBufferSize = false
	})
}

// DefaultSendBufferSize restores OS send buffer default.
func DefaultSendBufferSize() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setSendBufferSizeNative(handle, -1)
	}, func(options *SocketOptions) {
		options.SendBufferSize = OptionValue[int64]{}
		options.DefaultSendBufferSize = true
	})
}

// RecvBufferSize sets OS receive buffer size.
func RecvBufferSize(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("receive buffer size", bytes, setRecvBufferSizeNative, func(options *SocketOptions, value int64) {
		options.RecvBufferSize = OptionValue[int64]{Value: value, Set: true}
		options.DefaultRecvBufferSize = false
	})
}

// DefaultRecvBufferSize restores OS receive buffer default.
func DefaultRecvBufferSize() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setRecvBufferSizeNative(handle, -1)
	}, func(options *SocketOptions) {
		options.RecvBufferSize = OptionValue[int64]{}
		options.DefaultRecvBufferSize = true
	})
}

// XPubNoDrop toggles XPUB no-drop behavior.
func XPubNoDrop(enabled bool) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setXPubNoDropNative(handle, enabled)
	}, func(options *SocketOptions) {
		options.XPubNoDrop = OptionValue[bool]{Value: enabled, Set: true}
	})
}

// CompressionAutoTrain toggles compression dictionary auto-training.
func CompressionAutoTrain(enabled bool) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setCompressionAutoTrainNative(handle, enabled)
	}, func(options *SocketOptions) {
		options.CompressionAutoTrain = OptionValue[bool]{Value: enabled, Set: true}
	})
}

// CompressionThreshold sets minimum bytes before compression.
func CompressionThreshold(bytes int) SocketOption {
	return trackedNonNegativeInt64Option("compression threshold", int64(bytes), setCompressionThresholdNative, func(options *SocketOptions, value int64) {
		options.CompressionThreshold = OptionValue[int64]{Value: value, Set: true}
		options.CompressionDefaultThreshold = false
	})
}

// CompressionDefaultThreshold restores native compression threshold.
func CompressionDefaultThreshold() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setCompressionThresholdNative(handle, -1)
	}, func(options *SocketOptions) {
		options.CompressionThreshold = OptionValue[int64]{}
		options.CompressionDefaultThreshold = true
	})
}

// CompressionLevel sets zstd compression level.
func CompressionLevel(level int) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		if level < zstdLevelMin || level > zstdLevelMax {
			return &ConfigError{Err: "zstd compression level must be -8..=4"}
		}
		return setCompressionLevelNative(handle, int64(level))
	}, func(options *SocketOptions) {
		options.CompressionLevel = OptionValue[int]{Value: level, Set: true}
		options.CompressionDefaultLevel = false
	})
}

// CompressionDefaultLevel restores native compression level.
func CompressionDefaultLevel() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setCompressionLevelNative(handle, defaultCompressionLevel)
	}, func(options *SocketOptions) {
		options.CompressionLevel = OptionValue[int]{}
		options.CompressionDefaultLevel = true
	})
}

// CompressionDict sets a static compression dictionary.
func CompressionDict(value []byte) SocketOption {
	copied := append([]byte(nil), value...)
	return trackedOption(func(handle *nativeSocket) error {
		if len(value) == 0 {
			return &ConfigError{Err: "compression dict must not be empty"}
		}
		if len(value) > compressionDictMaxBytes {
			return &ConfigError{Err: "compression dict length must be at most 8192 bytes"}
		}
		return setCompressionDictNative(handle, value)
	}, func(options *SocketOptions) {
		options.CompressionDict = OptionValue[[]byte]{Value: append([]byte(nil), copied...), Set: true}
		options.NoCompressionDict = false
	})
}

// NoCompressionDict clears the static compression dictionary.
func NoCompressionDict() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setCompressionDictNative(handle, nil)
	}, func(options *SocketOptions) {
		options.CompressionDict = OptionValue[[]byte]{}
		options.NoCompressionDict = true
	})
}

// CompressionDictCapacity sets compression dictionary capacity.
func CompressionDictCapacity(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("compression dictionary capacity", bytes, setCompressionDictCapacityNative, func(options *SocketOptions, value int64) {
		options.CompressionDictCapacity = OptionValue[int64]{Value: value, Set: true}
		options.DefaultCompressionDictCapacity = false
	})
}

// DefaultCompressionDictCapacity restores native dictionary capacity.
func DefaultCompressionDictCapacity() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setCompressionDictCapacityNative(handle, -1)
	}, func(options *SocketOptions) {
		options.CompressionDictCapacity = OptionValue[int64]{}
		options.DefaultCompressionDictCapacity = true
	})
}

// MaxRecvDictSize sets max received dictionary size.
func MaxRecvDictSize(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("max receive dictionary size", bytes, setMaxRecvDictSizeNative, func(options *SocketOptions, value int64) {
		options.MaxRecvDictSize = OptionValue[int64]{Value: value, Set: true}
		options.DefaultMaxRecvDictSize = false
	})
}

// DefaultMaxRecvDictSize restores native received dictionary size.
func DefaultMaxRecvDictSize() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setMaxRecvDictSizeNative(handle, -1)
	}, func(options *SocketOptions) {
		options.MaxRecvDictSize = OptionValue[int64]{}
		options.DefaultMaxRecvDictSize = true
	})
}

// CompressionOffloadThreshold sets compression offload threshold.
func CompressionOffloadThreshold(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("compression offload threshold", bytes, setCompressionOffloadThresholdNative, func(options *SocketOptions, value int64) {
		options.CompressionOffloadThreshold = OptionValue[int64]{Value: value, Set: true}
		options.NoCompressionOffload = false
	})
}

// NoCompressionOffload disables compression offload.
func NoCompressionOffload() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setCompressionOffloadThresholdNative(handle, -1)
	}, func(options *SocketOptions) {
		options.CompressionOffloadThreshold = OptionValue[int64]{}
		options.NoCompressionOffload = true
	})
}

// LargeMessageThreshold sets large-message path threshold.
func LargeMessageThreshold(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("large message threshold", bytes, setLargeMessageThresholdNative, func(options *SocketOptions, value int64) {
		options.LargeMessageThreshold = OptionValue[int64]{Value: value, Set: true}
		options.DisableLargeMessagePath = false
	})
}

// DisableLargeMessagePath disables the large-message path.
func DisableLargeMessagePath() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setLargeMessageThresholdNative(handle, -1)
	}, func(options *SocketOptions) {
		options.LargeMessageThreshold = OptionValue[int64]{}
		options.DisableLargeMessagePath = true
	})
}

// ArenaThreshold sets native frame arena threshold.
func ArenaThreshold(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("arena threshold", bytes, setArenaThresholdNative, func(options *SocketOptions, value int64) {
		options.ArenaThreshold = OptionValue[int64]{Value: value, Set: true}
		options.DefaultArenaThreshold = false
	})
}

// DefaultArenaThreshold restores native arena threshold.
func DefaultArenaThreshold() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setArenaThresholdNative(handle, -1)
	}, func(options *SocketOptions) {
		options.ArenaThreshold = OptionValue[int64]{}
		options.DefaultArenaThreshold = true
	})
}

// TransmitSlotCapacity sets native transmit slot capacity.
func TransmitSlotCapacity(bytes int64) SocketOption {
	return trackedNonNegativeInt64Option("transmit slot capacity", bytes, setTransmitSlotCapacityNative, func(options *SocketOptions, value int64) {
		options.TransmitSlotCapacity = OptionValue[int64]{Value: value, Set: true}
		options.DefaultTransmitSlotCapacity = false
	})
}

// DefaultTransmitSlotCapacity restores native transmit slot capacity.
func DefaultTransmitSlotCapacity() SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		return setTransmitSlotCapacityNative(handle, -1)
	}, func(options *SocketOptions) {
		options.TransmitSlotCapacity = OptionValue[int64]{}
		options.DefaultTransmitSlotCapacity = true
	})
}

func durationOption(value time.Duration, set func(*nativeSocket, int64) error) SocketOption {
	return trackedDurationOption(value, set, nil)
}

func trackedDurationOption(
	value time.Duration,
	set func(*nativeSocket, int64) error,
	record func(*SocketOptions, time.Duration),
) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		millis, err := nonNegativeMillis("duration", value)
		if err != nil {
			return err
		}
		return set(handle, millis)
	}, func(options *SocketOptions) {
		if record != nil {
			record(options, value)
		}
	})
}

func nonNegativeMillis(name string, value time.Duration) (int64, error) {
	if value < 0 {
		return 0, &ConfigError{Err: name + " must be non-negative"}
	}
	return durationMillis(value), nil
}

func nonNegativeInt64Option(name string, value int64, set func(*nativeSocket, int64) error) SocketOption {
	return trackedNonNegativeInt64Option(name, value, set, nil)
}

func trackedNonNegativeInt64Option(
	name string,
	value int64,
	set func(*nativeSocket, int64) error,
	record func(*SocketOptions, int64),
) SocketOption {
	return trackedOption(func(handle *nativeSocket) error {
		if value < 0 {
			return &ConfigError{Err: name + " must be non-negative"}
		}
		return set(handle, value)
	}, func(options *SocketOptions) {
		if record != nil {
			record(options, value)
		}
	})
}

func validateZmtpShortString(name, value string) error {
	if len([]byte(value)) > zmtpMaxShortStringBytes {
		return &ConfigError{Err: name + " length must be at most 255 bytes"}
	}
	return nil
}

func authOption(
	auth Authenticator,
	option func(uint64) SocketOption,
	record func(*SocketOptions),
) SocketOption {
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
		s.recordOption(record)
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
