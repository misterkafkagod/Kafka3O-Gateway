package identity

import (
	"encoding/base64"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

func HashLocalPassword(random io.Reader, password string) (string, error) {
	if err := ValidateLocalPassword(password); err != nil {
		return "", err
	}
	if random == nil {
		return "", ErrRandomSource
	}
	salt := make([]byte, argonSaltBytes)
	if _, err := io.ReadFull(random, salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, argonIterations, argonMemoryKiB, argonParallelism, argonKeyBytes)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemoryKiB, argonIterations, argonParallelism,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key),
	), nil
}
