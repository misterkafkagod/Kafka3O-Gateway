package identity

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateLocalAccountCursor(t *testing.T) {
	for _, cursor := range []string{"", "user-1", "opaque_ID", strings.Repeat("x", 128)} {
		if err := ValidateLocalAccountCursor(cursor); err != nil {
			t.Fatalf("valid cursor rejected: %v", err)
		}
	}
	for _, cursor := range []string{" cursor", "cursor ", "cur\nsor", "cur\x00sor", string([]byte{0xff}), strings.Repeat("x", 129)} {
		if err := ValidateLocalAccountCursor(cursor); !errors.Is(err, ErrInvalidAccountCursor) {
			t.Fatalf("invalid cursor accepted: %v", err)
		}
	}
}
