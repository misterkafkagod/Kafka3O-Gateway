package identity

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCredentialHashesAreOmittedFromJSON(t *testing.T) {
	for _, value := range []any{
		LocalAccount{PasswordHash: "sensitive-hash"},
		LocalAuthenticationResult{PasswordHash: "sensitive-hash"},
		LocalPasswordUpdate{PreviousHash: "sensitive-hash", NewHash: "sensitive-hash"},
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), "sensitive-hash") {
			t.Fatal("serialized account data contains a password hash")
		}
	}
}
