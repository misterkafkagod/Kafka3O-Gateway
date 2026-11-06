package identity

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeLocalUsername(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
	}{
		{input: "ADMIN", want: "admin"},
		{input: "john.smith-2_ops", want: "john.smith-2_ops"},
		{input: "a", want: "a"},
	} {
		got, err := NormalizeLocalUsername(tt.input)
		if err != nil || got != tt.want {
			t.Errorf("username %q = %q, %v; want %q", tt.input, got, err, tt.want)
		}
	}
	for _, invalid := range []string{"", "-admin", "user name", "admin\n", "a/b", "Jöhn", strings.Repeat("x", 65)} {
		if _, err := NormalizeLocalUsername(invalid); !errors.Is(err, ErrInvalidUsername) {
			t.Errorf("username %q: got %v", invalid, err)
		}
	}
}
