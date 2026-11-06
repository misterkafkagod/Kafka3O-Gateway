package identity

import (
	"encoding/base64"
	"fmt"
	"io"
)

func newOpaqueID(random io.Reader) (string, error) {
	value := make([]byte, 16)
	if _, err := io.ReadFull(random, value); err != nil {
		return "", fmt.Errorf("generate identity: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
