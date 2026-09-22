package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// FuzzAPIKeyHeader proves the X-Api-Key parser never panics on arbitrary
// input and that only an exact digest match ever authenticates (TECH-SPEC
// §4.6, Step 11 G2).
func FuzzAPIKeyHeader(f *testing.F) {
	for _, seed := range []string{
		"", "operator-secret", "reader-secret", "operator-secre", "operator-secretX",
		"\x00\x01\x02", "a very long key " + string(make([]byte, 1000)),
		"日本語のキー", "'; DROP TABLE keys; --", "operator-secret\x00trailing",
	} {
		f.Add(seed)
	}

	keys := testKeys()

	f.Fuzz(func(t *testing.T, presented string) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
		req.Header.Set(HeaderAPIKey, presented)

		var caller struct{ ran bool }
		APIKey(keys, true, nil, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			caller.ran = true
		})).ServeHTTP(rec, req)

		if caller.ran && presented != "operator-secret" && presented != "reader-secret" {
			t.Fatalf("presented key %q authenticated but matches no configured digest", presented)
		}
		if !caller.ran && (presented == "operator-secret" || presented == "reader-secret") {
			t.Fatalf("presented key %q is a configured key but was rejected", presented)
		}
	})
}
