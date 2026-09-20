package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
)

// runKeygen is `kafka3o-gateway keygen --tier reader|operator` (TECH-SPEC
// §6.1 B4): prints a fresh API key and the SHA-256 digest an operator
// pastes into KGW_API_KEYS — the raw key itself is never stored.
func runKeygen(args []string) int {
	fs := flag.NewFlagSet("keygen", flag.ContinueOnError)
	tier := fs.String("tier", "", "key tier: reader or operator")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *tier != "reader" && *tier != "operator" {
		fmt.Fprintln(os.Stderr, "kafka3o-gateway keygen: --tier must be reader or operator")
		return 2
	}

	key, digest, err := generateKey()
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o-gateway keygen:", err)
		return 1
	}

	fmt.Printf("tier:   %s\nkey:    %s\nsha256: %s\n", *tier, key, digest)
	return 0
}

// generateKey draws 32 random bytes (TECH-SPEC B4) and returns the
// base64url-encoded key an operator presents, and the hex SHA-256 digest
// that goes into KGW_API_KEYS instead.
func generateKey() (key, sha256Hex string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("keygen: %w", err)
	}
	sum := sha256.Sum256(buf)
	return base64.RawURLEncoding.EncodeToString(buf), hex.EncodeToString(sum[:]), nil
}
