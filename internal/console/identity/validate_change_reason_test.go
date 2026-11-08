package identity

import (
	"errors"
	"testing"
)

func TestValidateChangeReason(t *testing.T) {
	if err := ValidateChangeReason("Incident 42 hotfix"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateChangeReason(""); !errors.Is(err, ErrChangeReasonRequired) {
		t.Fatalf("empty reason error = %v", err)
	}
}
