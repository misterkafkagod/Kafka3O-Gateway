package cognito

import (
	"strings"
	"testing"
)

func TestCognitoRefreshTokenValidation(t *testing.T) {
	for _, tt := range []struct {
		name, token string
		valid       bool
	}{
		{"printable", "synthetic-token", true},
		{"maximum length", strings.Repeat("x", MaxRefreshTokenBytes), true},
		{"empty", "", false},
		{"oversized", strings.Repeat("x", MaxRefreshTokenBytes+1), false},
		{"control", "bad\nvalue", false},
		{"delete", "bad\x7fvalue", false},
		{"non-ASCII", "badévalue", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validRefreshToken(tt.token); got != tt.valid {
				t.Fatalf("refresh token validation = %t, want %t", got, tt.valid)
			}
		})
	}
}
