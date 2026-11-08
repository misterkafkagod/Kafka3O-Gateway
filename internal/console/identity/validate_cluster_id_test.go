package identity

import (
	"errors"
	"testing"
)

func TestValidateClusterID(t *testing.T) {
	if err := ValidateClusterID("cluster-a"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", "cluster a", "cluster/a"} {
		if err := ValidateClusterID(id); !errors.Is(err, ErrInvalidClusterID) {
			t.Fatalf("ValidateClusterID(%q) = %v", id, err)
		}
	}
}
