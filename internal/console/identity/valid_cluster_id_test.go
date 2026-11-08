package identity

import (
	"strings"
	"testing"
)

func TestValidClusterID(t *testing.T) {
	for _, tt := range []struct {
		id    string
		valid bool
	}{
		{"cluster-a", true}, {"Kafka_B.2", true}, {strings.Repeat("c", 128), true},
		{"", false}, {strings.Repeat("c", 129), false}, {"cluster a", false},
		{"cluster/a", false}, {"clüster", false}, {"cluster\n", false},
	} {
		if got := validClusterID(tt.id); got != tt.valid {
			t.Errorf("validClusterID(%q) = %t, want %t", tt.id, got, tt.valid)
		}
	}
}
