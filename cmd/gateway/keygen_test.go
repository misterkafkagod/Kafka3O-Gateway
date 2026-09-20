package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestKeygen_Produces32ByteKeyAndMatchingSHA256(t *testing.T) {
	t.Parallel()

	key, digest, err := generateKey()
	if err != nil {
		t.Fatalf("generateKey() error: %v", err)
	}

	raw, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		t.Fatalf("key is not base64url: %v", err)
	}
	if len(raw) != 32 {
		t.Errorf("key decodes to %d bytes, want 32", len(raw))
	}

	sum, err := hex.DecodeString(digest)
	if err != nil {
		t.Fatalf("digest is not hex: %v", err)
	}
	if len(sum) != 32 {
		t.Errorf("digest decodes to %d bytes, want 32", len(sum))
	}

	wantSum := sha256.Sum256(raw)
	gotDigest := hex.EncodeToString(wantSum[:])
	if gotDigest != digest {
		t.Errorf("digest = %s, want sha256(key) = %s", digest, gotDigest)
	}
}

func TestKeygen_TwoRunsDiffer(t *testing.T) {
	t.Parallel()

	key1, digest1, err := generateKey()
	if err != nil {
		t.Fatalf("generateKey() error: %v", err)
	}
	key2, digest2, err := generateKey()
	if err != nil {
		t.Fatalf("generateKey() error: %v", err)
	}

	if key1 == key2 {
		t.Error("two generateKey() calls produced the same key")
	}
	if digest1 == digest2 {
		t.Error("two generateKey() calls produced the same digest")
	}
}
