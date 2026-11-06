package identity

import (
	"context"
	"errors"
	"fmt"
)

func (s createLocalAccount) failLocalAccountCreation(ctx context.Context, attempt LocalAccountCreationAttempt, eventID string, cause error) error {
	err := s.store.FailLocalAccountCreation(ctx, LocalAccountCreationFailure{
		Actor: attempt.Actor, AccountID: attempt.AccountID, OperationID: attempt.OperationID,
		EventID: eventID, OccurredAt: s.now(),
		Rejected: errors.Is(cause, ErrInvalidUsername) || errors.Is(cause, ErrInvalidPassword),
	})
	if errors.Is(err, ErrAccountCreationExpired) {
		return errors.Join(cause, err)
	}
	if err != nil {
		return errors.Join(ErrAccountCreationAudit, cause, fmt.Errorf("record failed local account creation: %w", err))
	}
	return cause
}
