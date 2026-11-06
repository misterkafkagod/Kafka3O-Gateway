package identity

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

type failedRandom struct{}

func (failedRandom) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestHashLocalPasswordUsesSaltAndVerifies(t *testing.T) {
	password := "a strong passphrase with spaces"
	first, err := HashLocalPassword(bytes.NewReader(bytes.Repeat([]byte{1}, argonSaltBytes)), password)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashLocalPassword(bytes.NewReader(bytes.Repeat([]byte{2}, argonSaltBytes)), password)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || strings.Contains(first, password) {
		t.Fatal("password hashes do not use unique salts or expose the password")
	}
	for _, hash := range []string{first, second} {
		matches, err := VerifyLocalPassword(password, hash)
		if err != nil || !matches {
			t.Fatalf("valid password: matches=%t err=%v", matches, err)
		}
		matches, err = VerifyLocalPassword("wrong password", hash)
		if err != nil || matches {
			t.Fatalf("wrong password: matches=%t err=%v", matches, err)
		}
	}
}

func TestHashLocalPasswordRejectsBadInputsAndEntropyFailures(t *testing.T) {
	for _, password := range []string{"", "short", strings.Repeat(" ", 12), strings.Repeat("a", passwordMaxBytes+1), string([]byte{0xff})} {
		_, err := HashLocalPassword(bytes.NewReader(make([]byte, argonSaltBytes)), password)
		if !errors.Is(err, ErrInvalidPassword) {
			t.Errorf("invalid password: got %v", err)
		}
	}
	if _, err := HashLocalPassword(nil, "valid passphrase"); !errors.Is(err, ErrRandomSource) {
		t.Errorf("missing random source: got %v", err)
	}
	if _, err := HashLocalPassword(failedRandom{}, "valid passphrase"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("failed random source: got %v", err)
	}
}
