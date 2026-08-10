package omq

import (
	"sync"
	"sync/atomic"
)

type Authenticator func(PeerInfo) bool

var (
	nextAuthCallbackID atomic.Uint64
	authCallbacksMu    sync.RWMutex
	authCallbacks      = make(map[uint64]Authenticator)
)

func registerAuthCallback(callback Authenticator) uint64 {
	id := nextAuthCallbackID.Add(1)
	authCallbacksMu.Lock()
	authCallbacks[id] = callback
	authCallbacksMu.Unlock()
	return id
}

func unregisterAuthCallback(id uint64) {
	authCallbacksMu.Lock()
	delete(authCallbacks, id)
	authCallbacksMu.Unlock()
}

func callAuthCallback(id uint64, peer PeerInfo) bool {
	authCallbacksMu.RLock()
	callback := authCallbacks[id]
	authCallbacksMu.RUnlock()
	if callback == nil {
		return false
	}
	return callback(peer)
}
