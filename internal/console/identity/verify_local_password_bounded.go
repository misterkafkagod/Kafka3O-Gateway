package identity

import "context"

func verifyLocalPasswordBounded(ctx context.Context, slots chan struct{}, password, hash string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return VerifyLocalPassword(password, hash)
}
