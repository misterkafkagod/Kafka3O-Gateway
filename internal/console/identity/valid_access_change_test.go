package identity

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

func TestValidAccessChange(t *testing.T) {
	for _, tt := range []struct {
		name           string
		cluster        string
		expected, role authorization.Role
		valid          bool
	}{
		{"global grant", "", authorization.RoleNone, authorization.RoleViewer, true},
		{"cluster removal", "cluster-a", authorization.RoleAdmin, authorization.RoleNone, true},
		{"invalid cluster", "cluster a", authorization.RoleNone, authorization.RoleViewer, false},
		{"unknown role", "", authorization.RoleNone, "OWNER", false},
		{"unknown expected role", "", "admin", authorization.RoleViewer, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validAccessChange(tt.cluster, tt.expected, tt.role); got != tt.valid {
				t.Fatalf("validAccessChange = %t, want %t", got, tt.valid)
			}
		})
	}
}
