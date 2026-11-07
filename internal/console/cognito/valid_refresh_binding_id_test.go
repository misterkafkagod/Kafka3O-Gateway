package cognito

import (
	"strings"
	"testing"
)

func TestCognitoRefreshBindingIDValidation(t *testing.T) {
	for _, tt := range []struct {
		name, id string
		valid    bool
	}{
		{"simple", "session-1", true},
		{"maximum length", strings.Repeat("x", 128), true},
		{"empty", "", false},
		{"oversized", strings.Repeat("x", 129), false},
		{"leading space", " session", false},
		{"trailing space", "session ", false},
		{"control", "ses\tsion", false},
		{"invalid UTF-8", "ses\xffsion", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validRefreshBindingID(tt.id); got != tt.valid {
				t.Fatalf("refresh binding ID validation = %t, want %t", got, tt.valid)
			}
		})
	}
}
