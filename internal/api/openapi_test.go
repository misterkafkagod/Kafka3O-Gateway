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

	implemented := map[string]bool{
		"C1": true, "C2": true, "C3": true, "C4": true,
		"T1": true, "T2": true, "T3": true, "T4": true,
		"M1": true, "M2": true, "M3": true, "M4": true, "M5": true, "M6": true, "M7": true,
		"G1": true, "G2": true, "G3": true,
		"T5": true, "T6": true, "T7": true, "T8": true, "T9": true, "T10": true, "T11": true, "T12": true,
		"G4": true, "G5": true, "G6": true, "G7": true,
		"M8": true,
		"C5": true, "C6": true, "C7": true, "C8": true, "C9": true, "C10": true, "C11": true, "C12": true,
		"S1": true, "S2": true,
	}
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

func TestOpenAPI_PendingListEmpty(t *testing.T) {
	t.Parallel()
	if got := api.Pending(); len(got) != 0 {
		t.Errorf("Pending() = %v, want empty (FUNC-SPEC §9.7 O1: every catalog command is now wired)", got)
	}
}

// TestOpenAPI_41IDsOver48Operations locks in the operation count TASKS.md's
// Phase 13 Manual Test Plan step 5 names. TECH-SPEC §6.2's own route table
// is the authoritative source: 41 rows (12 C + 12 T + 8 M + 7 G + 2 S), four
// of which carry more than one operation — C3 x2, C9 x3, S1 x3, S2 x2 (+1
// each beyond the 37 single-operation ids' own +1) — summing to 47, not the
// 48 TECH-SPEC §6.1 B2's prose states; the prose figure does not match its
// own table and this test follows the table.
func TestOpenAPI_41IDsOver48Operations(t *testing.T) {
	t.Parallel()
	ids := commandIDsOf(t, fetchOpenAPI(t))

	unique := map[string]bool{}
	total := 0
	for _, id := range ids {
		if id == "" {
			continue
		}
		unique[id] = true
		total++
	}
	if len(unique) != 41 {
		t.Errorf("unique x-command-id count = %d, want 41", len(unique))
	}
	if total != 47 {
		t.Errorf("operations carrying an x-command-id = %d, want 47 (TECH-SPEC §6.2 route table: C3 x2, C9 x3, S1 x3, S2 x2)", total)
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
