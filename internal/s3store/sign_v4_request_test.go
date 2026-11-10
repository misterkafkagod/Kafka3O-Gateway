package s3store

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// AWS Signature Version 4 test suite, "get-vanilla" (published example credentials, not a secret).
func TestSignV4RequestMatchesTheAWSGetVanillaVector(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "https://example.amazonaws.com/", nil)
	SignV4Request(request, Credentials{AccessKey: "AKIDEXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"},
		"us-east-1", "service", time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC))
	want := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, " +
		"Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got := request.Header.Get("Authorization"); got != want {
		t.Fatalf("authorization =\n%s\nwant\n%s", got, want)
	}
	if request.Header.Get("X-Amz-Date") != "20150830T123600Z" {
		t.Fatalf("date = %s", request.Header.Get("X-Amz-Date"))
	}
}

func TestSignV4RequestSignsTheContentHashAndSortedQuery(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "http://s3:8333/kafka3o-backups?prefix=kafka-a%2Forders%2F&list-type=2", nil)
	request.Header.Set("X-Amz-Content-Sha256", EmptyPayloadHash)
	SignV4Request(request, Credentials{AccessKey: "a", SecretKey: "b"}, "us-east-1", "s3", time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC))
	got := request.Header.Get("Authorization")
	if want := "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"; !strings.Contains(got, want) {
		t.Fatalf("authorization %q lacks %q", got, want)
	}
}
