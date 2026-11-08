package storage

import "testing"

func TestValidAuditState(t *testing.T) {
	for _, tt := range []struct {
		state string
		valid bool
	}{
		{"", true}, {"VIEWER", true}, {"OPERATOR", true}, {"ADMIN", true},
		{"NON_PRODUCTION", true}, {"PRODUCTION", true},
		{"admin", false}, {"OWNER", false}, {"PRODUCTION\n", false}, {"password", false},
	} {
		if got := validAuditState(tt.state); got != tt.valid {
			t.Errorf("validAuditState(%q) = %t, want %t", tt.state, got, tt.valid)
		}
	}
}
