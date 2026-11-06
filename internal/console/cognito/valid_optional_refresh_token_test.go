package cognito

import (
	"strings"
	"testing"
)

func TestCognitoOptionalRefreshTokenResponseValidation(t *testing.T) {
	for _, tt := range []struct {
		name, contentType, body string
		valid                   bool
	}{
		{"missing JSON token", "application/json", `{"access_token":"access"}`, true},
		{"valid JSON token", "application/json", `{"refresh_token":"synthetic-token"}`, true},
		{"JSON null token", "application/json", `{"refresh_token":null}`, false},
		{"JSON empty token", "application/json", `{"refresh_token":""}`, false},
		{"JSON wrong token type", "application/json", `{"refresh_token":12}`, false},
		{"JSON token control", "application/json", `{"refresh_token":"bad\nvalue"}`, false},
		{"JSON oversized token", "application/json", `{"refresh_token":"` + strings.Repeat("x", MaxRefreshTokenBytes+1) + `"}`, false},
		{"missing form token", "application/x-www-form-urlencoded", "access_token=access", true},
		{"valid form token", "application/x-www-form-urlencoded", "refresh_token=synthetic-token", true},
		{"empty form token", "text/plain", "refresh_token=", false},
		{"duplicate form token", "application/x-www-form-urlencoded", "refresh_token=one&refresh_token=two", false},
		{"invalid form token", "text/plain", "refresh_token=bad%0Avalue", false},
		{"malformed JSON delegated", "application/json", "{", true},
		{"malformed form delegated", "application/x-www-form-urlencoded", "refresh_token=%xx", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validOptionalRefreshToken(tt.contentType, []byte(tt.body)); got != tt.valid {
				t.Fatalf("optional refresh token validation = %t, want %t", got, tt.valid)
			}
		})
	}
}
