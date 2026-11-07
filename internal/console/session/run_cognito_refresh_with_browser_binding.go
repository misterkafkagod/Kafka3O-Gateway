package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (m *Manager) runCognitoRefreshWithBrowserBinding(tokenHash, browserHash [32]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), cognitoRefreshTimeout)
	defer cancel()
	coordinator := m.cognitoRefresh
	var boundStore BoundCognitoRefreshStore
	var state CognitoRefreshState
	var err error
	if browserHash != ([32]byte{}) {
		var ok bool
		boundStore, ok = coordinator.store.(BoundCognitoRefreshStore)
		if !ok {
			return ErrInvalidSessionConfig
		}
		state, err = boundStore.InspectBoundCognitoRefresh(ctx, tokenHash, browserHash, m.now().UTC())
	} else {
		state, err = coordinator.store.InspectCognitoRefresh(ctx, tokenHash, m.now().UTC())
	}
	if err == ErrInvalidSession {
		return ErrInvalidSession
	}
	if err != nil {
		return errors.Join(ErrCognitoRefreshUnavailable, err)
	}
	switch state {
	case CognitoRefreshFresh:
		return nil
	case CognitoRefreshBlocked:
		return ErrCognitoRefreshUnavailable
	case CognitoRefreshReady:
	default:
		return ErrCognitoRefreshUnavailable
	}
	var result cognito.RefreshExchangeResult
	if boundStore != nil {
		result, err = coordinator.exchanger.RefreshBoundCognitoSession(ctx, tokenHash, browserHash, m.cognitoProtector)
	} else {
		result, err = coordinator.exchanger.RefreshCognitoSession(ctx, tokenHash, m.cognitoProtector)
	}
	if err != nil {
		return errors.Join(ErrCognitoRefreshUnavailable, fmt.Errorf("Cognito refresh operation %s: %w", result.OperationID, err))
	}
	proof, err := result.ForCompletion(m.cognitoProtector, m.now().UTC())
	if err != nil {
		return errors.Join(ErrCognitoRefreshUnavailable, fmt.Errorf("Cognito refresh operation %s: %w", result.OperationID, err))
	}
	completion := cognito.RefreshCompletionRequest{Refresh: proof, Protector: m.cognitoProtector, OccurredAt: m.now().UTC()}
	if boundStore != nil {
		err = boundStore.CompleteBoundCognitoRefresh(ctx, completion, browserHash)
	} else {
		err = coordinator.store.CompleteCognitoRefresh(ctx, completion)
	}
	if err != nil {
		return errors.Join(ErrCognitoRefreshUnavailable, fmt.Errorf("Cognito refresh operation %s: %w", result.OperationID, err))
	}
	return nil
}
