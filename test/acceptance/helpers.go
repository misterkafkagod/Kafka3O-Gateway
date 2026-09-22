//go:build acceptance

package acceptance

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// doAcceptance issues a request against the running gateway, presenting
// apiKey (FUNC-SPEC B3: no endpoint is exempt from authentication).
func doAcceptance(t *testing.T, method, path, apiKey string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, baseURL+path, nil)
	if err != nil {
		t.Fatalf("http.NewRequest(%s %q) error: %v", method, path, err)
	}
	req.Header.Set("X-Api-Key", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s error: %v", method, path, err)
	}
	return resp
}

// doAcceptanceJSON is doAcceptance for a request carrying a JSON body (M3's
// search, M4's filter).
func doAcceptanceJSON(t *testing.T, method, path, apiKey string, body any) *http.Response {
	t.Helper()

	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal(body) error: %v", err)
	}
	req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("http.NewRequest(%s %q) error: %v", method, path, err)
	}
	req.Header.Set("X-Api-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s error: %v", method, path, err)
	}
	return resp
}

// drainAndClose discards resp's body and closes it — every test that does
// not need the body must still call this so the connection can be reused.
func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
