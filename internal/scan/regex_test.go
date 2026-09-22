package scan_test

import (
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/scan"
)

func TestRegexMatcher_FieldsValueKeyHeaders(t *testing.T) {
	t.Parallel()

	valueMatcher, err := scan.NewRegexMatcher("FOUND", []scan.RegexField{scan.FieldValue}, false, 0)
	if err != nil {
		t.Fatalf("NewRegexMatcher() error: %v", err)
	}
	if !valueMatcher(scan.Record{Value: "xFOUNDx"}).Match {
		t.Error("value-field matcher should match a value containing the pattern")
	}
	if valueMatcher(scan.Record{Value: "nope", Key: "xFOUNDx"}).Match {
		t.Error("value-field matcher should not match a key containing the pattern")
	}

	keyMatcher, err := scan.NewRegexMatcher("FOUND", []scan.RegexField{scan.FieldKey}, false, 0)
	if err != nil {
		t.Fatalf("NewRegexMatcher() error: %v", err)
	}
	if !keyMatcher(scan.Record{Key: "xFOUNDx"}).Match {
		t.Error("key-field matcher should match a key containing the pattern")
	}

	headersMatcher, err := scan.NewRegexMatcher("FOUND", []scan.RegexField{scan.FieldHeaders}, false, 0)
	if err != nil {
		t.Fatalf("NewRegexMatcher() error: %v", err)
	}
	if !headersMatcher(scan.Record{Headers: []scan.Header{{Value: "xFOUNDx"}}}).Match {
		t.Error("headers-field matcher should match a header value containing the pattern")
	}
}

func TestRegexMatcher_CaseInsensitive(t *testing.T) {
	t.Parallel()

	insensitive, err := scan.NewRegexMatcher("found", nil, true, 0)
	if err != nil {
		t.Fatalf("NewRegexMatcher() error: %v", err)
	}
	if !insensitive(scan.Record{Value: "FOUND"}).Match {
		t.Error("case-insensitive matcher should match FOUND against pattern found")
	}

	sensitive, err := scan.NewRegexMatcher("found", nil, false, 0)
	if err != nil {
		t.Fatalf("NewRegexMatcher() error: %v", err)
	}
	if sensitive(scan.Record{Value: "FOUND"}).Match {
		t.Error("case-sensitive matcher should not match FOUND against pattern found")
	}
}

func TestRegexMatcher_InvalidPatternError(t *testing.T) {
	t.Parallel()
	if _, err := scan.NewRegexMatcher("(", nil, false, 0); err == nil {
		t.Fatal("NewRegexMatcher(invalid pattern) returned nil error")
	}
}

func TestRegexMatcher_NestedQuantifierCompletesWithinBound(t *testing.T) {
	t.Parallel()
	m, err := scan.NewRegexMatcher(`(a+)+$`, nil, false, 0)
	if err != nil {
		t.Fatalf("NewRegexMatcher() error: %v", err)
	}
	pathological := strings.Repeat("a", 40) + "!"

	start := time.Now()
	m(scan.Record{Value: pathological})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("nested-quantifier match took %s, want RE2's linear time (fast, not exponential backtracking)", elapsed)
	}
}
