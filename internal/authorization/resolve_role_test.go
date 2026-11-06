package authorization

import (
	"errors"
	"testing"
)

func TestResolveRole(t *testing.T) {
	for _, tt := range []struct {
		name      string
		grants    []Grant
		clusterID string
		want      Role
	}{
		{name: "no grants", clusterID: "a", want: RoleNone},
		{name: "global applies to cluster", grants: []Grant{{Role: RoleViewer}}, clusterID: "a", want: RoleViewer},
		{name: "cluster elevates global", grants: []Grant{{Role: RoleViewer}, {ClusterID: "a", Role: RoleAdmin}}, clusterID: "a", want: RoleAdmin},
		{name: "cluster cannot reduce global", grants: []Grant{{Role: RoleAdmin}, {ClusterID: "a", Role: RoleViewer}}, clusterID: "a", want: RoleAdmin},
		{name: "other cluster excluded", grants: []Grant{{ClusterID: "b", Role: RoleAdmin}}, clusterID: "a", want: RoleNone},
		{name: "cluster does not become global", grants: []Grant{{ClusterID: "a", Role: RoleAdmin}}, want: RoleNone},
		{name: "highest independent of order", grants: []Grant{{ClusterID: "a", Role: RoleOperator}, {Role: RoleAdmin}, {ClusterID: "a", Role: RoleViewer}}, clusterID: "a", want: RoleAdmin},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveRole(tt.grants, tt.clusterID)
			if err != nil || got != tt.want {
				t.Fatalf("ResolveRole() = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestResolveRoleRejectsInvalidGrants(t *testing.T) {
	for _, role := range []Role{RoleNone, "admin", "OWNER"} {
		_, err := ResolveRole([]Grant{{Role: RoleAdmin}, {ClusterID: "other", Role: role}}, "a")
		if !errors.Is(err, ErrInvalidRole) {
			t.Errorf("role %q: got %v, want invalid-role error", role, err)
		}
	}
}
