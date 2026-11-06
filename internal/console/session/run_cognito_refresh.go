package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (m *Manager) runCognitoRefresh(tokenHash [32]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), cognitoRefreshTimeout)
	defer cancel()
	coordinator := m.cognitoRefresh
	state, err := coordinator.store.InspectCognitoRefresh(ctx, tokenHash, m.now().UTC())
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
	result, err := coordinator.exchanger.RefreshCognitoSession(ctx, tokenHash, m.cognitoProtector)
	if err != nil {
		return errors.Join(ErrCognitoRefreshUnavailable, fmt.Errorf("Cognito refresh operation %s: %w", result.OperationID, err))
	}
	proof, err := result.ForCompletion(m.cognitoProtector, m.now().UTC())
	if err != nil {
		return errors.Join(ErrCognitoRefreshUnavailable, fmt.Errorf("Cognito refresh operation %s: %w", result.OperationID, err))
	}
	if err := coordinator.store.CompleteCognitoRefresh(ctx, cognito.RefreshCompletionRequest{
		Refresh: proof, Protector: m.cognitoProtector, OccurredAt: m.now().UTC(),
	}); err != nil {
		return errors.Join(ErrCognitoRefreshUnavailable, fmt.Errorf("Cognito refresh operation %s: %w", result.OperationID, err))
	}
	return nil
}
