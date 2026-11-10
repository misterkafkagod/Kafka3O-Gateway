package connect

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateConnectorName(t *testing.T) {
	for _, valid := range []string{"kafka3o-verify-running", "orders sink", "a.b_c-1", strings.Repeat("x", 249)} {
		if err := ValidateConnectorName(valid); err != nil {
			t.Errorf("%q: %v", valid, err)
		}
	}
	for _, invalid := range []string{"", " lead", "trail ", "a/b", "line\nbreak", "nul\x00", strings.Repeat("x", 250), "\xff"} {
		if err := ValidateConnectorName(invalid); !errors.Is(err, ErrInvalidConnectorName) {
			t.Errorf("%q was accepted", invalid)
		}
	}
}
