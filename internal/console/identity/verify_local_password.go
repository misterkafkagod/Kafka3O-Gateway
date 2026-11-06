package identity

import (
	"crypto/subtle"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

func VerifyLocalPassword(password, encoded string) (bool, error) {
	if len(password) > passwordMaxBytes || !utf8.ValidString(password) {
		return false, ErrInvalidPassword
	}
	hash, err := parseLocalPasswordHash(encoded)
	if err != nil {
		return false, err
	}
	key := argon2.IDKey([]byte(password), hash.salt, hash.iterations, hash.memory, hash.parallelism, uint32(len(hash.key)))
	return subtle.ConstantTimeCompare(key, hash.key) == 1, nil
}
