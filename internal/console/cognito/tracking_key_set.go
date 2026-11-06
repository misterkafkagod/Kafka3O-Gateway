package cognito

import (
	"context"

	"github.com/coreos/go-oidc/v3/oidc"
)

type trackingKeySet struct {
	keys oidc.KeySet
}

func (k trackingKeySet) VerifySignature(ctx context.Context, rawToken string) ([]byte, error) {
	payload, err := k.keys.VerifySignature(ctx, rawToken)
	if attempt, ok := ctx.Value(verificationAttemptKey{}).(*verificationAttempt); ok {
		attempt.keyError = err
	}
	return payload, err
}
