package session

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type failingBrowserMarkerReader struct {
	err error
}

func (r failingBrowserMarkerReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestBrowserMarkerGenerationAndDomainSeparation(t *testing.T) {
	raw := bytes.Repeat([]byte{27}, 32)
	value, hash, err := NewBrowserMarker(bytes.NewReader(raw))
	if err != nil || value != base64.RawURLEncoding.EncodeToString(raw) || len(value) != 43 || hash == ([32]byte{}) {
		t.Fatal("browser marker generation returned invalid data")
	}
	ordinary := sha256.Sum256(raw)
	data := append([]byte("Kafka3O stable browser marker v1\x00"), ordinary[:]...)
	expected := sha256.Sum256(data)
	if hash != expected || hash == ordinary {
		t.Fatal("browser marker digest was not domain separated from session tokens")
	}
	if !BrowserMarkerMatchesSessionValue(hash, ordinary) || !BrowserMarkerMatchesSessionValue(ordinary, ordinary) ||
		BrowserMarkerMatchesSessionValue([32]byte{}, ordinary) ||
		BrowserMarkerMatchesSessionValue(hash, sha256.Sum256([]byte("different value"))) {
		t.Fatal("browser marker did not detect reused session values")
	}
	readHash, err := HashBrowserMarker(value)
	if err != nil || readHash != hash {
		t.Fatal("generated marker did not reproduce its digest")
	}
	otherValue, otherHash, err := NewBrowserMarker(bytes.NewReader(bytes.Repeat([]byte{28}, 32)))
	if err != nil || otherValue == value || otherHash == hash {
		t.Fatal("independent marker entropy produced the same marker or digest")
	}
	for _, random := range []io.Reader{nil, bytes.NewReader(nil), bytes.NewReader(raw[:31]), failedRandom{}} {
		generated, digest, err := NewBrowserMarker(random)
		if err == nil || generated != "" || digest != ([32]byte{}) {
			t.Fatal("entropy failure returned a browser marker")
		}
		if strings.Contains(err.Error(), value) {
			t.Fatal("marker generation error exposed a marker")
		}
	}
}

func TestBrowserMarkerEntropyErrorPreservesCauseWithoutExposingValues(t *testing.T) {
	secret := "synthetic-sensitive-marker-value"
	cause := errors.New(secret)
	value, hash, err := NewBrowserMarker(failingBrowserMarkerReader{err: cause})
	if !errors.Is(err, cause) || value != "" || hash != ([32]byte{}) {
		t.Fatal("entropy failure did not preserve its cause without marker output")
	}
	encoded, encodeErr := json.Marshal(err)
	if encodeErr != nil || strings.Contains(err.Error(), secret) || strings.Contains(fmt.Sprint(err), secret) ||
		strings.Contains(fmt.Sprintf("%#v", err), secret) || bytes.Contains(encoded, []byte(secret)) {
		t.Fatal("entropy failure exposed sensitive source details")
	}
}

func TestBrowserMarkerRejectsNoncanonicalValues(t *testing.T) {
	value := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{17}, 32))
	for _, malformed := range []string{
		"", value[:42], value + "=", value + "A", value[:42] + "B",
		value[:20] + "+" + value[21:], value[:20] + "/" + value[21:],
		" " + value, value + " ", `"` + value + `"`, value + "\n", strings.Repeat("x", 4096),
	} {
		hash, err := HashBrowserMarker(malformed)
		if !errors.Is(err, ErrInvalidBrowserMarker) || hash != ([32]byte{}) {
			t.Fatal("malformed browser marker was accepted")
		}
		if strings.Contains(err.Error(), malformed) && len(malformed) > 0 {
			t.Fatal("browser marker error exposed submitted data")
		}
	}
}

func TestBrowserMarkerCookieRequiresOneCanonicalRawField(t *testing.T) {
	value := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{19}, 32))
	other := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{20}, 32))
	name := BrowserMarkerCookieName
	expected, err := HashBrowserMarker(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		headers []string
		valid   bool
	}{
		{"missing", nil, false},
		{"unrelated", []string{"other=value"}, false},
		{"empty", []string{name + "="}, false},
		{"missing equals", []string{name}, false},
		{"invalid value", []string{name + "=bad"}, false},
		{"space before equals", []string{name + " =" + value}, false},
		{"space after equals", []string{name + "= " + value}, false},
		{"quoted value", []string{name + `="` + value + `"`}, false},
		{"noncanonical", []string{name + "=" + value[:42] + "B"}, false},
		{"same duplicate", []string{name + "=" + value + "; " + name + "=" + value}, false},
		{"different duplicate", []string{name + "=" + value + "; " + name + "=" + other}, false},
		{"malformed duplicate", []string{name + "=" + value + "; " + name + `="unterminated`}, false},
		{"missing equals duplicate", []string{name + "=" + value + "; " + name}, false},
		{"spaced duplicate", []string{name + "=" + value + "; " + name + " =" + value}, false},
		{"multiple headers", []string{name + "=" + value, name + "=" + value}, false},
		{"valid", []string{"other=value; " + name + "=" + value + "; another=value"}, true},
		{"valid across headers", []string{"other=value", name + "=" + value}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "https://console.example/callback", nil)
			for _, header := range tt.headers {
				request.Header.Add("Cookie", header)
			}
			hash, err := ReadBrowserMarkerCookie(request)
			if tt.valid {
				if err != nil || hash != expected {
					t.Fatal("canonical browser marker cookie was rejected")
				}
			} else if !errors.Is(err, ErrInvalidBrowserMarker) || hash != ([32]byte{}) {
				t.Fatal("missing or ambiguous browser marker cookie was accepted")
			} else if strings.Contains(err.Error(), value) || strings.Contains(err.Error(), other) {
				t.Fatal("browser marker cookie error exposed a value")
			}
		})
	}
	if hash, err := ReadBrowserMarkerCookie(nil); !errors.Is(err, ErrInvalidBrowserMarker) || hash != ([32]byte{}) {
		t.Fatal("nil marker request was accepted")
	}
}
