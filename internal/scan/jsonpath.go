package scan

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/theory/jsonpath"
)

// FilterOp is one JSONPath filter comparison operator (FUNC-SPEC V4).
type FilterOp string

// FilterOp values.
const (
	OpEq       FilterOp = "eq"
	OpNeq      FilterOp = "neq"
	OpContains FilterOp = "contains"
	OpRegex    FilterOp = "regex"
	OpExists   FilterOp = "exists"
	OpGt       FilterOp = "gt"
	OpLt       FilterOp = "lt"
	OpGte      FilterOp = "gte"
	OpLte      FilterOp = "lte"
)

// NewJSONPathMatcher compiles path (RFC 9535, via theory/jsonpath) and
// returns a Matcher: match if any node path selects from the record's
// parsed JSON value satisfies op against value (FUNC-SPEC V4). An invalid
// path, or an invalid regex value for op == OpRegex, reports an error the
// caller maps to 400 INVALID_JSONPATH. A record whose value is not valid
// JSON is skipped, never an error (FUNC-SPEC §9.2 Evaluate); a path that
// selects nothing is a plain non-match, not a skip; a type mismatch (e.g.
// OpGt against a string node) is also a non-match, not a skip.
func NewJSONPathMatcher(path string, op FilterOp, value any) (Matcher, error) {
	p, err := jsonpath.Parse(path)
	if err != nil {
		return nil, err
	}

	var valueRegex *regexp.Regexp
	if op == OpRegex {
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("scan: op %q requires a string value", op)
		}
		valueRegex, err = regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
	}

	return func(r Record) MatchResult {
		var parsed any
		if err := json.Unmarshal([]byte(r.Value), &parsed); err != nil {
			return MatchResult{Skipped: true}
		}

		nodes := p.Select(parsed)
		if op == OpExists {
			return MatchResult{Match: len(nodes) > 0}
		}
		for _, node := range nodes {
			if evalOp(op, node, value, valueRegex) {
				return MatchResult{Match: true}
			}
		}
		return MatchResult{}
	}, nil
}

// evalOp tests one selected node against op and value.
func evalOp(op FilterOp, node, value any, valueRegex *regexp.Regexp) bool {
	switch op {
	case OpEq:
		return jsonEqual(node, value)
	case OpNeq:
		return !jsonEqual(node, value)
	case OpContains:
		return jsonContains(node, value)
	case OpRegex:
		s, ok := node.(string)
		return ok && valueRegex.MatchString(s)
	case OpGt:
		nf, vf, ok := bothFloat64(node, value)
		return ok && nf > vf
	case OpLt:
		nf, vf, ok := bothFloat64(node, value)
		return ok && nf < vf
	case OpGte:
		nf, vf, ok := bothFloat64(node, value)
		return ok && nf >= vf
	case OpLte:
		nf, vf, ok := bothFloat64(node, value)
		return ok && nf <= vf
	case OpExists:
		return true // handled by the caller before any node is evaluated
	default:
		return false
	}
}

// jsonEqual compares two decoded JSON values without ever invoking Go's ==
// on a potentially uncomparable dynamic type (map/slice) — mismatched or
// unsupported types are simply unequal, never a panic.
func jsonEqual(a, b any) bool {
	if af, bf, ok := bothFloat64(a, b); ok {
		return af == bf
	}
	if as, ok := a.(string); ok {
		bs, ok := b.(string)
		return ok && as == bs
	}
	if ab, ok := a.(bool); ok {
		bb, ok := b.(bool)
		return ok && ab == bb
	}
	return a == nil && b == nil
}

// jsonContains implements "contains": substring for a string node, element
// membership (by jsonEqual) for an array node.
func jsonContains(node, value any) bool {
	switch n := node.(type) {
	case string:
		s, ok := value.(string)
		return ok && strings.Contains(n, s)
	case []any:
		for _, elem := range n {
			if jsonEqual(elem, value) {
				return true
			}
		}
	}
	return false
}

// bothFloat64 reports a and b as float64 only when both are JSON numbers.
func bothFloat64(a, b any) (af, bf float64, ok bool) {
	af, aOK := a.(float64)
	bf, bOK := b.(float64)
	return af, bf, aOK && bOK
}
