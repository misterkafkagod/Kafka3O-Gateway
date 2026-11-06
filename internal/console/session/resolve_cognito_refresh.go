package session

import (
	"context"
	"errors"
)

func (m *Manager) resolveCognitoRefresh(ctx context.Context, tokenHash [32]byte) (Principal, error) {
	coordinator := m.cognitoRefresh
	coordinator.mu.Lock()
	call := coordinator.active[tokenHash]
	if call == nil {
		if len(coordinator.active) >= maxCognitoRefreshes {
			coordinator.mu.Unlock()
			return Principal{}, ErrCognitoRefreshUnavailable
		}
		call = &cognitoRefreshCall{done: make(chan struct{})}
		coordinator.active[tokenHash] = call
		go func() {
			err := m.runCognitoRefresh(tokenHash)
			coordinator.mu.Lock()
			call.err = err
			close(call.done)
			delete(coordinator.active, tokenHash)
			coordinator.mu.Unlock()
		}()
	}
	coordinator.mu.Unlock()
	select {
	case <-ctx.Done():
		return Principal{}, ctx.Err()
	case <-call.done:
	}
	if call.err != nil {
		return Principal{}, call.err
	}
	principal, err := m.store.UseSession(ctx, tokenHash, m.now, m.idleLifetime, m.cognitoProtector)
	if err == ErrInvalidSession {
		_, inspectErr := coordinator.store.InspectCognitoRefresh(ctx, tokenHash, m.now().UTC())
		if inspectErr == ErrInvalidSession {
			return Principal{}, ErrInvalidSession
		}
		return Principal{}, errors.Join(ErrCognitoRefreshUnavailable, inspectErr)
	}
	return principal, err
}
