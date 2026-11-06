package identity

import (
	"context"
	"errors"
	"fmt"
)

func (b bootstrapLocalAdmin) failBootstrapAttempt(ctx context.Context, eventID, operationID string, cause error) error {
	err := b.store.FailBootstrapAttempt(ctx, BootstrapFailure{
		EventID: eventID, OperationID: operationID, OccurredAt: b.now(),
		Denied: errors.Is(cause, ErrInvalidBootstrapSecret) || errors.Is(cause, ErrInvalidUsername) ||
			errors.Is(cause, ErrInvalidPassword) || errors.Is(cause, ErrAlreadyBootstrapped),
	})
	if err != nil {
		return errors.Join(cause, fmt.Errorf("record failed bootstrap: %w", err))
	}
	return cause
}
