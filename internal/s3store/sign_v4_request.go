package s3store

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"
)

// EmptyPayloadHash is the SHA-256 of an empty body, sent as x-amz-content-sha256 on bodiless S3 requests.
const EmptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// SignV4Request adds X-Amz-Date and an AWS Signature Version 4 Authorization header for a bodiless request. It signs
// host, x-amz-date and, when present, x-amz-content-sha256.
func SignV4Request(request *http.Request, credentials Credentials, region, service string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	day := amzDate[:8]
	request.Header.Set("X-Amz-Date", amzDate)
	payloadHash := request.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = EmptyPayloadHash
	}
	headers := map[string]string{"host": request.Host, "x-amz-date": amzDate}
	if request.Host == "" {
		headers["host"] = request.URL.Host
	}
	if value := request.Header.Get("X-Amz-Content-Sha256"); value != "" {
		headers["x-amz-content-sha256"] = value
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name + ":" + strings.TrimSpace(headers[name]) + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	query := request.URL.Query()
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := []string{}
	for _, key := range keys {
		values := append([]string(nil), query[key]...)
		sort.Strings(values)
		for _, value := range values {
			pairs = append(pairs, EncodeURIComponent(key, false)+"="+EncodeURIComponent(value, false))
		}
	}
	path := request.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonicalRequest := strings.Join([]string{request.Method, path, strings.Join(pairs, "&"), canonicalHeaders.String(), signedHeaders, payloadHash}, "\n")
	digest := sha256.Sum256([]byte(canonicalRequest))
	scope := day + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(digest[:])
	key := HMACSHA256(HMACSHA256(HMACSHA256(HMACSHA256([]byte("AWS4"+credentials.SecretKey), day), region), service), "aws4_request")
	signature := hex.EncodeToString(HMACSHA256(key, stringToSign))
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+credentials.AccessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}
