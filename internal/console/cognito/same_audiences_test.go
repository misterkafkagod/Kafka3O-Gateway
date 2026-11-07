package cognito

import (
	"slices"
	"testing"
)

func TestCognitoSameAudiences(t *testing.T) {
	for _, tt := range []struct {
		name          string
		first, second []string
		same          bool
	}{
		{"identical", []string{"a", "b"}, []string{"a", "b"}, true},
		{"reordered", []string{"b", "a"}, []string{"a", "b"}, true},
		{"different length", []string{"a"}, []string{"a", "b"}, false},
		{"different member", []string{"a", "c"}, []string{"a", "b"}, false},
		{"duplicate count", []string{"a", "a"}, []string{"a", "b"}, false},
		{"both empty", nil, []string{}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			first, second := slices.Clone(tt.first), slices.Clone(tt.second)
			if got := sameAudiences(tt.first, tt.second); got != tt.same {
				t.Fatalf("sameAudiences = %t, want %t", got, tt.same)
			}
			if !slices.Equal(tt.first, first) || !slices.Equal(tt.second, second) {
				t.Fatal("sameAudiences modified its arguments")
			}
		})
	}
}
