package session

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

func newSessionValue(random io.Reader, size int) (string, [32]byte, error) {
	value := make([]byte, size)
	if _, err := io.ReadFull(random, value); err != nil {
		return "", [32]byte{}, fmt.Errorf("generate session value: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), sha256.Sum256(value), nil
}
