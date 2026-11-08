package identity

import (
	"strings"
	"testing"
)

func TestValidGroupMapping(t *testing.T) {
	issuer := "https://cognito-idp.eu-west-1.amazonaws.com/eu-west-1_Example"
	for _, tt := range []struct {
		name, issuer, group string
		valid               bool
	}{
		{"exact issuer", issuer, "kafka-admins", true},
		{"case-sensitive group", issuer, "Kafka Admins", true},
		{"HTTP issuer", "http://issuer.example", "group", false},
		{"issuer query", issuer + "?x=1", "group", false},
		{"issuer fragment", issuer + "#x", "group", false},
		{"issuer credentials", "https://user@issuer.example", "group", false},
		{"long issuer", "https://" + strings.Repeat("i", 260), "group", false},
		{"empty group", issuer, "", false},
		{"padded group", issuer, " group", false},
		{"control group", issuer, "gr\toup", false},
		{"long group", issuer, strings.Repeat("g", 129), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validGroupMapping(tt.issuer, tt.group); got != tt.valid {
				t.Fatalf("validGroupMapping = %t, want %t", got, tt.valid)
			}
		})
	}
}
