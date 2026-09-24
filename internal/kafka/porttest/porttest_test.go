package porttest

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "update golden files")

// TestPortTest_CaseListStable pins the contract suite's case names (TECH-SPEC
// L1). The fake run in CI and the franz run at Level 2 both execute these
// same functions, so this list is the case list each run reports; a change
// to it is a deliberate contract change, made with -update.
func TestPortTest_CaseListStable(t *testing.T) {
	t.Parallel()
	caseName := regexp.MustCompile(`t\.Run\("([^"]+)"`)

	var got []string
	for _, file := range []string{"suite.go", "admin.go", "consumer.go", "producer.go"} {
		src, err := os.ReadFile(file) //nolint:gosec // G304: fixed list of this package's own source files
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, m := range caseName.FindAllStringSubmatch(string(src), -1) {
			got = append(got, file+": "+m[1])
		}
	}
	gotText := strings.Join(got, "\n") + "\n"

	golden := filepath.Join("testdata", "cases.golden.txt")
	if *update {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(golden, []byte(gotText), 0o600); err != nil { //nolint:gosec // G703: fixed testdata path, -update only
			t.Fatalf("write golden: %v", err)
		}
	}
	want, err := os.ReadFile(golden) //nolint:gosec // G304: fixed testdata path
	if err != nil {
		t.Fatalf("read golden: %v (run: go test ./internal/kafka/porttest -update)", err)
	}
	if strings.ReplaceAll(string(want), "\r\n", "\n") != gotText {
		t.Errorf("porttest case list changed; if intended, run: go test ./internal/kafka/porttest -update\ngot:\n%s", gotText)
	}
}
