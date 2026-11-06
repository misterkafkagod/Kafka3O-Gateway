package identity

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestVerifyLocalPasswordRejectsMalformedAndUnboundedHashes(t *testing.T) {
	hash, err := HashLocalPassword(bytes.NewReader(make([]byte, argonSaltBytes)), "valid passphrase")
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		"", strings.Repeat("x", 1025),
		strings.Replace(hash, "argon2id", "argon2i", 1),
		strings.Replace(hash, "v=19", "v=16", 1),
		strings.Replace(hash, "m=19456", "m=1", 1),
		strings.Replace(hash, "m=19456", "m=999999999", 1),
		strings.Replace(hash, "t=2", "t=999", 1),
		strings.Replace(hash, "p=1", "p=0", 1),
		strings.Replace(hash, "m=19456", "m=019456", 1),
		hash + "$extra", hash[:len(hash)-1],
	} {
		matches, err := VerifyLocalPassword("valid passphrase", invalid)
		if matches || !errors.Is(err, ErrInvalidPasswordHash) {
			t.Errorf("malformed hash: matches=%t err=%v", matches, err)
		}
	}
	if matches, err := VerifyLocalPassword(strings.Repeat("a", passwordMaxBytes+1), hash); matches || !errors.Is(err, ErrInvalidPassword) {
		t.Errorf("oversized password: matches=%t err=%v", matches, err)
	}
}

func TestVerifyLocalPasswordChecksShortWrongPasswords(t *testing.T) {
	hash, err := HashLocalPassword(bytes.NewReader(make([]byte, argonSaltBytes)), "valid passphrase")
	if err != nil {
		t.Fatal(err)
	}
	matches, err := VerifyLocalPassword("", hash)
	if err != nil || matches {
		t.Fatalf("empty password: matches=%t err=%v", matches, err)
	}
}
