package api_test

import (
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/api"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

var update = flag.Bool("update", false, "update golden files")

func fetchOpenAPI(t *testing.T) map[string]any {
	t.Helper()
	gw := testutil.NewTestGateway(t)

	resp := gw.Get(t, "/openapi.json")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("decode OpenAPI document: %v", err)
	}
	return doc
}

// commandIDsOf walks every operation in doc's paths and returns the
// x-command-id extension value for each, keyed by "METHOD PATH".
func commandIDsOf(t *testing.T, doc map[string]any) map[string]string {
	t.Helper()
	ids := map[string]string{}

	paths, _ := doc["paths"].(map[string]any)
	for path, rawItem := range paths {
		item, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		for _, method := range []string{"get", "post", "put", "patch", "delete", "head", "options", "trace"} {
			op, ok := item[method].(map[string]any)
			if !ok {
				continue
			}
			id, _ := op["x-command-id"].(string)
			ids[method+" "+path] = id
		}
	}
	return ids
}

func TestOpenAPI_EveryOperationHasExactlyOneKnownCommandID(t *testing.T) {
	t.Parallel()
	ids := commandIDsOf(t, fetchOpenAPI(t))

	known := map[string]bool{}
	for _, d := range command.Table() {
		known[d.ID] = true
	}

	if len(ids) == 0 {
		t.Fatal("no operations found in the OpenAPI document")
	}
	for op, id := range ids {
		if id == "" {
			t.Errorf("%s: missing x-command-id", op)
			continue
		}
		if !known[id] {
			t.Errorf("%s: x-command-id %q is not a command.Table id", op, id)
		}
	}
}

func TestOpenAPI_EveryNonPendingIDHasAnOperation(t *testing.T) {
	t.Parallel()
	ids := commandIDsOf(t, fetchOpenAPI(t))

	present := map[string]bool{}
	for _, id := range ids {
		present[id] = true
	}
	pending := map[string]bool{}
	for _, id := range api.Pending() {
		pending[id] = true
	}

	for _, d := range command.Table() {
		if pending[d.ID] {
			continue
		}
		if !present[d.ID] {
			t.Errorf("command %s is not pending but has no operation", d.ID)
		}
	}
}

func TestOpenAPI_PendingIsTableMinusImplemented(t *testing.T) {
	t.Parallel()

	got := append([]string(nil), api.Pending()...)
	sort.Strings(got)

	implemented := map[string]bool{"C1": true, "C2": true, "C3": true, "C4": true, "T1": true, "T2": true, "T3": true, "T4": true}
	want := make([]string, 0, len(command.Table()))
	for _, d := range command.Table() {
		if implemented[d.ID] {
			continue
		}
		want = append(want, d.ID)
	}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("Pending() has %d ids, want %d: got=%v want=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Pending()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestOpenAPI_Golden(t *testing.T) {
	t.Parallel()
	doc := fetchOpenAPI(t)

	got, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent() error: %v", err)
	}
	got = append(got, '\n')

	golden := filepath.Join("testdata", "openapi.golden.json")
	if *update {
		if err := os.WriteFile(golden, got, 0o600); err != nil {
			t.Fatalf("write golden file: %v", err)
		}
	}

	want, err := os.ReadFile(golden) //nolint:gosec // G304: golden is a fixed testdata path built from constants, not request input
	if err != nil {
		t.Fatalf("read golden file: %v (run: go test ./internal/api/... -run TestOpenAPI_Golden -update)", err)
	}
	if string(got) != string(want) {
		t.Errorf("OpenAPI document does not match %s (run: go test ./internal/api/... -run TestOpenAPI_Golden -update)", golden)
	}
}
