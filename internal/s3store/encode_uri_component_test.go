package s3store

import "testing"

func TestEncodeURIComponent(t *testing.T) {
	for _, tt := range []struct {
		in        string
		keepSlash bool
		want      string
	}{
		{"kafka-a/orders/", true, "kafka-a/orders/"},
		{"kafka-a/orders/", false, "kafka-a%2Forders%2F"},
		{"a b+c~d", false, "a%20b%2Bc~d"},
		{"é", false, "%C3%A9"},
	} {
		if got := EncodeURIComponent(tt.in, tt.keepSlash); got != tt.want {
			t.Errorf("%q = %q, want %q", tt.in, got, tt.want)
		}
	}
}
