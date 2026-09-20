package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// probeAddr is the gateway's own loopback address (TECH-SPEC §6.1 B3): the
// probe is an exec probe running inside the same container, on the single
// exposed port 8080.
const probeAddr = "http://127.0.0.1:8080"

// probeAPIKeyEnv holds the reader-tier key probes present (TECH-SPEC B3):
// no endpoint, including /health/*, is exempt from authentication.
//
//nolint:gosec // G101: this is an environment variable *name*, not a credential value.
const probeAPIKeyEnv = "KGW_PROBE_API_KEY"

const probeTimeout = 5 * time.Second

// runProbe is `kafka3o-gateway probe --live|--ready` (TECH-SPEC §6.1 B3).
func runProbe(args, environ []string) int {
	path, ok := probePath(args)
	if !ok {
		fmt.Fprintln(os.Stderr, "kafka3o-gateway probe: exactly one of --live or --ready is required")
		return 2
	}
	return probeCheck(probeAddr, path, envValue(environ, probeAPIKeyEnv))
}

// probePath maps --live/--ready onto the endpoint path. ok is false when
// neither or both are given.
func probePath(args []string) (path string, ok bool) {
	live, ready := false, false
	for _, a := range args {
		switch a {
		case "--live":
			live = true
		case "--ready":
			ready = true
		}
	}
	switch {
	case live && !ready:
		return "/health/live", true
	case ready && !live:
		return "/health/ready", true
	default:
		return "", false
	}
}

// probeCheck performs the exec probe's HTTP GET (TECH-SPEC B3):
// baseURL + "/v1" + path, presenting apiKey when non-empty. It returns 0
// on a 200 response, 1 otherwise — wrong key, a non-200 status (including
// 503 from a DOWN /health/ready), or a transport error.
func probeCheck(baseURL, path, apiKey string) int {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1"+path, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o-gateway probe:", err)
		return 1
	}
	if apiKey != "" {
		req.Header.Set("X-Api-Key", apiKey)
	}

	client := &http.Client{Timeout: probeTimeout}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o-gateway probe:", err)
		return 1
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

// envValue returns the value of name in environ, or "" if unset.
func envValue(environ []string, name string) string {
	prefix := name + "="
	for _, e := range environ {
		if v, ok := strings.CutPrefix(e, prefix); ok {
			return v
		}
	}
	return ""
}
