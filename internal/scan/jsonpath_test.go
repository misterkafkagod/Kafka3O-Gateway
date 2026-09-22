package scan_test

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/scan"
)

func TestJSONPathMatcher_Op(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		json  string
		op    scan.FilterOp
		value any
		want  bool
	}{
		{"eq_Match", `{"v":"FAILED"}`, scan.OpEq, "FAILED", true},
		{"eq_NoMatch", `{"v":"OK"}`, scan.OpEq, "FAILED", false},
		{"neq_Match", `{"v":"OK"}`, scan.OpNeq, "FAILED", true},
		{"neq_NoMatch", `{"v":"FAILED"}`, scan.OpNeq, "FAILED", false},
		{"contains_Match", `{"v":"a FAILED b"}`, scan.OpContains, "FAILED", true},
		{"contains_NoMatch", `{"v":"a OK b"}`, scan.OpContains, "FAILED", false},
		{"regex_Match", `{"v":"FAILED-123"}`, scan.OpRegex, "^FAILED", true},
		{"regex_NoMatch", `{"v":"OK-123"}`, scan.OpRegex, "^FAILED", false},
		{"exists_Match", `{"v":"anything"}`, scan.OpExists, nil, true},
		{"exists_NoMatch", `{"other":1}`, scan.OpExists, nil, false},
		{"gt_Match", `{"v":5}`, scan.OpGt, float64(3), true},
		{"gt_NoMatch", `{"v":2}`, scan.OpGt, float64(3), false},
		{"lt_Match", `{"v":1}`, scan.OpLt, float64(3), true},
		{"lt_NoMatch", `{"v":5}`, scan.OpLt, float64(3), false},
		{"gte_Match", `{"v":3}`, scan.OpGte, float64(3), true},
		{"gte_NoMatch", `{"v":2}`, scan.OpGte, float64(3), false},
		{"lte_Match", `{"v":3}`, scan.OpLte, float64(3), true},
		{"lte_NoMatch", `{"v":4}`, scan.OpLte, float64(3), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := scan.NewJSONPathMatcher("$.v", tc.op, tc.value)
			if err != nil {
				t.Fatalf("NewJSONPathMatcher() error: %v", err)
			}
			result := m(scan.Record{Value: tc.json})
			if result.Match != tc.want {
				t.Errorf("Match = %v, want %v (result=%+v)", result.Match, tc.want, result)
			}
			if result.Skipped {
				t.Error("Skipped = true, want false for valid JSON")
			}
		})
	}
}

func TestJSONPathMatcher_MissingPathNoMatch(t *testing.T) {
	t.Parallel()
	m, err := scan.NewJSONPathMatcher("$.missing", scan.OpEq, "x")
	if err != nil {
		t.Fatalf("NewJSONPathMatcher() error: %v", err)
	}
	result := m(scan.Record{Value: `{"other":"x"}`})
	if result.Match || result.Skipped {
		t.Errorf("result = %+v, want a plain non-match", result)
	}
}

func TestJSONPathMatcher_TypeMismatchNoMatch(t *testing.T) {
	t.Parallel()
	m, err := scan.NewJSONPathMatcher("$.v", scan.OpGt, float64(1))
	if err != nil {
		t.Fatalf("NewJSONPathMatcher() error: %v", err)
	}
	result := m(scan.Record{Value: `{"v":"not a number"}`})
	if result.Match || result.Skipped {
		t.Errorf("result = %+v, want a plain non-match (type mismatch), not a skip", result)
	}
}

func TestJSONPathMatcher_NonJSONSkipped(t *testing.T) {
	t.Parallel()
	m, err := scan.NewJSONPathMatcher("$.v", scan.OpEq, "x")
	if err != nil {
		t.Fatalf("NewJSONPathMatcher() error: %v", err)
	}
	result := m(scan.Record{Value: "not json at all"})
	if !result.Skipped || result.Match {
		t.Errorf("result = %+v, want Skipped=true, Match=false", result)
	}
}

func TestJSONPathMatcher_InvalidPathError(t *testing.T) {
	t.Parallel()
	if _, err := scan.NewJSONPathMatcher("not a valid path", scan.OpEq, "x"); err == nil {
		t.Fatal("NewJSONPathMatcher(invalid path) returned nil error")
	}
}

func TestJSONPathMatcher_AnyNodeSatisfies(t *testing.T) {
	t.Parallel()
	m, err := scan.NewJSONPathMatcher("$.items[*].status", scan.OpEq, "FAILED")
	if err != nil {
		t.Fatalf("NewJSONPathMatcher() error: %v", err)
	}
	result := m(scan.Record{Value: `{"items":[{"status":"OK"},{"status":"FAILED"}]}`})
	if !result.Match {
		t.Errorf("result = %+v, want Match=true (the second item satisfies)", result)
	}
}
