// Command acceptance-report turns a `go test -json` stream from the Level 2
// acceptance run into the release checklist document (TECH-SPEC §4.8, T2):
//
//	go test -json -tags acceptance,integration ./test/... ./internal/kafka/franz/... \
//	  | go run ./tools/acceptance-report -version v1.2.3
//
// It writes docs/acceptance/<version>-<date>.md with one row per catalog
// command (id, name, result, duration, broker version, run id) and exits
// non-zero if any command is missing, failed, or was only skipped — a
// skipped test verifies nothing, so it cannot count as release evidence.
//
// The tool is standard-library only (depguard), so the catalog lives in
// catalog.tsv; internal/command's tests keep it identical to command.Table.
package main

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

//go:embed catalog.tsv
//nolint:gochecknoglobals // go:embed requires a package-level variable; read-only data, not state (D2)
var catalogTSV string

// contractTest is the franz porttest run under the integration tag (Task 16.2).
const contractTest = "TestFranz_PortContract"

// Result is one command's (or the contract suite's) outcome.
type Result string

// Result values.
const (
	Pass    Result = "PASS"
	Fail    Result = "FAIL"
	Skip    Result = "SKIP"
	Missing Result = "MISSING"
)

// Command is one catalog entry.
type Command struct {
	ID   string
	Name string
}

// Row is one line of the report.
type Row struct {
	Command
	Result   Result
	Duration time.Duration
}

// Report is everything the Markdown document shows.
type Report struct {
	GatewayVersion string
	Date           string
	BrokerVersion  string
	RunID          string
	Rows           []Row
	Contract       Result
	ContractCases  int
}

// testEvent is the subset of `go test -json` (test2json) the tool reads.
type testEvent struct {
	Action  string
	Test    string
	Elapsed float64
	Output  string
}

// outcome accumulates every test mapped to one id.
type outcome struct {
	passed, failed, skipped int
	elapsed                 time.Duration
}

// run is main without os.Exit, for tests.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("acceptance-report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	version := fs.String("version", "dev", "gateway version (file name and header)")
	date := fs.String("date", time.Now().UTC().Format("2006-01-02"), "report date, YYYY-MM-DD")
	outDir := fs.String("out", filepath.Join("docs", "acceptance"), "output directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	catalog, err := parseCatalog(catalogTSV)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "acceptance-report:", err)
		return 1
	}
	report, problems, err := build(catalog, stdin, *version, *date)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "acceptance-report:", err)
		return 1
	}

	if err := os.MkdirAll(*outDir, 0o750); err != nil {
		_, _ = fmt.Fprintln(stderr, "acceptance-report:", err)
		return 1
	}
	path := filepath.Join(*outDir, fmt.Sprintf("%s-%s.md", *version, *date))
	if err := os.WriteFile(path, []byte(render(report)), 0o600); err != nil {
		_, _ = fmt.Fprintln(stderr, "acceptance-report:", err)
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "acceptance-report: wrote", path)

	for _, p := range problems {
		_, _ = fmt.Fprintln(stderr, "acceptance-report:", p)
	}
	if len(problems) > 0 {
		return 1
	}
	return 0
}

// parseCatalog reads catalog.tsv: one "<id>\t<name>" line per command.
func parseCatalog(tsv string) ([]Command, error) {
	var out []Command
	for i, line := range strings.Split(strings.TrimSpace(tsv), "\n") {
		id, name, ok := strings.Cut(strings.TrimRight(line, "\r"), "\t")
		if !ok || id == "" || name == "" {
			return nil, fmt.Errorf("catalog.tsv line %d: want <id>\\t<name>", i+1)
		}
		out = append(out, Command{ID: id, Name: name})
	}
	return out, nil
}

// stream is what one pass over the `go test -json` input collects.
type stream struct {
	outcomes      map[string]*outcome
	contract      outcome
	contractCases int
	runID, broker string
}

// build reads the event stream and assembles the report. problems lists
// every reason the run is not release evidence; an error means the input
// itself was unreadable.
func build(catalog []Command, events io.Reader, version, date string) (Report, []string, error) {
	s, err := scan(events)
	if err != nil {
		return Report{}, nil, err
	}
	report := Report{GatewayVersion: version, Date: date, RunID: s.runID, BrokerVersion: s.broker}

	var problems []string
	passed := 0
	for _, c := range catalog {
		row := Row{Command: c, Result: Missing}
		if o := s.outcomes[c.ID]; o != nil {
			row.Result, row.Duration = o.result(), o.elapsed
		}
		if row.Result == Pass {
			passed++
		} else {
			problems = append(problems, fmt.Sprintf("%s (%s): %s", c.ID, c.Name, row.Result))
		}
		report.Rows = append(report.Rows, row)
	}

	report.Contract, report.ContractCases = Missing, s.contractCases
	if s.contract != (outcome{}) {
		report.Contract = s.contract.result()
	}
	if report.Contract != Pass {
		problems = append(problems, fmt.Sprintf("franz contract suite (%s): %s", contractTest, report.Contract))
	}
	if report.RunID == "" {
		problems = append(problems, "no ACCEPTANCE-META line: run id and broker version unknown")
		report.RunID, report.BrokerVersion = "unknown", "unknown"
	}
	if passed != len(catalog) {
		problems = append([]string{fmt.Sprintf("%d of %d commands passed", passed, len(catalog))}, problems...)
	}
	return report, problems, nil
}

// scan reads the whole `go test -json` stream once.
func scan(events io.Reader) (stream, error) {
	// metaLine is printed once by test/acceptance's TestMain.
	metaLine := regexp.MustCompile(`ACCEPTANCE-META run-id=(\S+) broker-version=(\S+)`)
	// acceptanceTest matches the top-level TestAcceptance_<ID>_<Name> tests.
	acceptanceTest := regexp.MustCompile(`^TestAcceptance_([A-Z][0-9]+)_[A-Za-z0-9_]+$`)
	s := stream{outcomes: map[string]*outcome{}}

	scanner := bufio.NewScanner(events)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var ev testEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			return stream{}, fmt.Errorf("not a go test -json stream: %w", err)
		}
		if m := metaLine.FindStringSubmatch(ev.Output); m != nil {
			s.runID, s.broker = m[1], m[2]
		}
		if ev.Action != "pass" && ev.Action != "fail" && ev.Action != "skip" {
			continue
		}
		switch {
		case ev.Test == contractTest:
			record(&s.contract, ev)
		case strings.HasPrefix(ev.Test, contractTest+"/"):
			if strings.Count(ev.Test, "/") == 2 {
				s.contractCases++
			}
		default:
			if m := acceptanceTest.FindStringSubmatch(ev.Test); m != nil {
				if s.outcomes[m[1]] == nil {
					s.outcomes[m[1]] = &outcome{}
				}
				record(s.outcomes[m[1]], ev)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return stream{}, fmt.Errorf("read input: %w", err)
	}
	return s, nil
}

func record(o *outcome, ev testEvent) {
	switch ev.Action {
	case "pass":
		o.passed++
	case "fail":
		o.failed++
	case "skip":
		o.skipped++
	}
	o.elapsed += time.Duration(ev.Elapsed * float64(time.Second))
}

// result is PASS only when every mapped test passed.
func (o *outcome) result() Result {
	switch {
	case o.failed > 0:
		return Fail
	case o.skipped > 0:
		return Skip
	case o.passed > 0:
		return Pass
	default:
		return Missing
	}
}

// render produces the Markdown document.
func render(r Report) string {
	var b strings.Builder
	passed := 0
	for _, row := range r.Rows {
		if row.Result == Pass {
			passed++
		}
	}
	overall := Pass
	if passed != len(r.Rows) || r.Contract != Pass {
		overall = Fail
	}

	fmt.Fprintf(&b, "# Acceptance report: kafka3o-gateway %s\n\n", r.GatewayVersion)
	fmt.Fprintf(&b, "| | |\n|---|---|\n")
	fmt.Fprintf(&b, "| Result | **%s**, %d of %d commands passed |\n", overall, passed, len(r.Rows))
	fmt.Fprintf(&b, "| Date | %s |\n", r.Date)
	fmt.Fprintf(&b, "| Gateway version | %s |\n", r.GatewayVersion)
	fmt.Fprintf(&b, "| Broker version | %s |\n", r.BrokerVersion)
	fmt.Fprintf(&b, "| Run id | %s |\n", r.RunID)
	fmt.Fprintf(&b, "| franz contract suite | %s (%d cases) |\n\n", r.Contract, r.ContractCases)

	fmt.Fprintf(&b, "| ID | Command | Result | Duration | Broker version | Run id |\n")
	fmt.Fprintf(&b, "|---|---|---|---|---|---|\n")
	for _, row := range r.Rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n",
			row.ID, row.Name, row.Result, row.Duration.Round(time.Millisecond), r.BrokerVersion, r.RunID)
	}
	return b.String()
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
