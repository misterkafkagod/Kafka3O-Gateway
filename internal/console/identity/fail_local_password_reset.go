package identity

import (
	"context"
	"errors"
	"fmt"
)

func (s resetLocalPassword) failLocalPasswordReset(ctx context.Context, attempt LocalPasswordResetAttempt, eventID string, cause error) error {
	err := s.store.FailLocalPasswordReset(ctx, LocalPasswordResetFailure{
		Actor: attempt.Actor, TargetID: attempt.TargetID, OperationID: attempt.OperationID,
		EventID: eventID, OccurredAt: s.now(), Rejected: errors.Is(cause, ErrInvalidPassword),
	})
	if err != nil {
		return errors.Join(cause, fmt.Errorf("record failed password reset: %w", err))
	}
	return cause
}
