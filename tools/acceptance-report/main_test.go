package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "update golden files")

func fixture(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "gotest.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return string(b)
}

func catalog(t *testing.T) []Command {
	t.Helper()
	c, err := parseCatalog(catalogTSV)
	if err != nil {
		t.Fatalf("parseCatalog: %v", err)
	}
	return c
}

// without drops every fixture line mentioning test.
func without(stream, test string) string {
	var kept []string
	for _, line := range strings.Split(stream, "\n") {
		if !strings.Contains(line, `"Test":"`+test+`"`) {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

func TestAcceptanceReport_MapsTestNamesToCatalogIDs(t *testing.T) {
	t.Parallel()
	cat := catalog(t)
	report, problems, err := build(cat, strings.NewReader(fixture(t)), "v0.1.0", "2026-09-24")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(problems) != 0 {
		t.Fatalf("problems = %v, want none", problems)
	}
	if len(cat) != 41 || len(report.Rows) != 41 {
		t.Fatalf("catalog %d / rows %d, want 41 each", len(cat), len(report.Rows))
	}
	for i, row := range report.Rows {
		if row.ID != cat[i].ID || row.Result != Pass {
			t.Errorf("row %d = %+v, want %s PASS in catalog order", i, row, cat[i].ID)
		}
	}
	if report.RunID != "mfk3x9q2" || report.BrokerVersion != "3.9.1" {
		t.Errorf("meta = %q / %q, want mfk3x9q2 / 3.9.1", report.RunID, report.BrokerVersion)
	}
	if report.Contract != Pass || report.ContractCases != 3 {
		t.Errorf("contract = %s (%d cases), want PASS (3)", report.Contract, report.ContractCases)
	}
	for _, row := range report.Rows {
		if row.ID == "C3" && row.Duration.Seconds() < 0.25 {
			t.Errorf("C3 duration %s does not include its second test", row.Duration)
		}
	}
}

func TestAcceptanceReport_FailsOnMissingID(t *testing.T) {
	t.Parallel()
	stream := without(fixture(t), "TestAcceptance_T7_Deletetopic")
	report, problems, err := build(catalog(t), strings.NewReader(stream), "v0.1.0", "2026-09-24")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, row := range report.Rows {
		if row.ID == "T7" && row.Result != Missing {
			t.Errorf("T7 = %s, want MISSING", row.Result)
		}
	}
	if !strings.Contains(strings.Join(problems, "\n"), "T7 (Delete topic): MISSING") {
		t.Errorf("problems = %v, want T7 reported missing", problems)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-out", t.TempDir()}, strings.NewReader(stream), &stdout, &stderr); code == 0 {
		t.Errorf("run() exit = 0 with T7 missing; stderr: %s", stderr.String())
	}
}

func TestAcceptanceReport_FailsOnAnyFail(t *testing.T) {
	t.Parallel()
	stream := strings.Replace(fixture(t),
		`"Action":"pass","Package":"github.com/misterkafkagod/kafka3o/test/acceptance","Test":"TestAcceptance_G5_Deleteconsumergroup"`,
		`"Action":"fail","Package":"github.com/misterkafkagod/kafka3o/test/acceptance","Test":"TestAcceptance_G5_Deleteconsumergroup"`, 1)
	report, problems, err := build(catalog(t), strings.NewReader(stream), "v0.1.0", "2026-09-24")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, row := range report.Rows {
		if row.ID == "G5" && row.Result != Fail {
			t.Errorf("G5 = %s, want FAIL", row.Result)
		}
	}
	if len(problems) == 0 {
		t.Fatal("problems = none, want G5 reported")
	}

	skipped := strings.Replace(fixture(t),
		`"Action":"pass","Package":"github.com/misterkafkagod/kafka3o/test/acceptance","Test":"TestAcceptance_C9_Partitionreassignmentleaderelection"`,
		`"Action":"skip","Package":"github.com/misterkafkagod/kafka3o/test/acceptance","Test":"TestAcceptance_C9_Partitionreassignmentleaderelection"`, 1)
	if _, problems, _ := build(catalog(t), strings.NewReader(skipped), "v0.1.0", "2026-09-24"); len(problems) == 0 {
		t.Error("a skipped command produced no problem; a skip is not release evidence")
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-out", t.TempDir()}, strings.NewReader(stream), &stdout, &stderr); code == 0 {
		t.Errorf("run() exit = 0 with G5 failed; stderr: %s", stderr.String())
	}
}

func TestAcceptanceReport_MarkdownGolden(t *testing.T) {
	t.Parallel()
	out := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-version", "v0.1.0", "-date", "2026-09-24", "-out", out},
		strings.NewReader(fixture(t)), &stdout, &stderr); code != 0 {
		t.Fatalf("run() exit = %d; stderr: %s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(out, "v0.1.0-2026-09-24.md")) //nolint:gosec // G304: a t.TempDir() path this test created
	if err != nil {
		t.Fatalf("read report: %v", err)
	}

	golden := filepath.Join("testdata", "report.golden.md")
	if *update {
		if err := os.WriteFile(golden, got, 0o600); err != nil { //nolint:gosec // G703: fixed testdata path, -update only
			t.Fatalf("write golden: %v", err)
		}
	}
	want, err := os.ReadFile(golden) //nolint:gosec // G304: fixed testdata path
	if err != nil {
		t.Fatalf("read golden: %v (run with -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("report differs from %s (run: go test ./tools/acceptance-report -update)\n%s", golden, got)
	}
}
