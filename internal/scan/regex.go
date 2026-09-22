package scan

import (
	"regexp"
	"strings"
	"time"
)

// RegexField selects which part of a record the regex matcher checks
// (FUNC-SPEC §8.7 M3).
type RegexField string

// RegexField values.
const (
	FieldValue   RegexField = "value"
	FieldKey     RegexField = "key"
	FieldHeaders RegexField = "headers"
)

// NewRegexMatcher compiles pattern (RE2 via regexp — linear time, immune to
// catastrophic backtracking) and returns a Matcher testing it against
// fields (default []{FieldValue}), case-insensitively when caseInsensitive
// is set (FUNC-SPEC §8.7 M3). An invalid pattern reports an error the caller
// maps to 400 INVALID_REGEX. timeout bounds each field's match call as a
// defence-in-depth guard beyond RE2's own linear-time guarantee (FUNC-SPEC
// §8.8 "regex per-message match timeout"); timeout <= 0 disables it. A
// per-record timeout is reported as MatchResult{Skipped: true}, never an
// error (FUNC-SPEC §9.2 Evaluate).
func NewRegexMatcher(pattern string, fields []RegexField, caseInsensitive bool, timeout time.Duration) (Matcher, error) {
	if caseInsensitive && !strings.HasPrefix(pattern, "(?i)") {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		fields = []RegexField{FieldValue}
	}

	return func(r Record) MatchResult {
		for _, field := range fields {
			for _, s := range regexFieldStrings(r, field) {
				matched, timedOut := regexMatch(re, s, timeout)
				if timedOut {
					return MatchResult{Skipped: true}
				}
				if matched {
					return MatchResult{Match: true}
				}
			}
		}
		return MatchResult{}
	}, nil
}

// regexFieldStrings returns the string(s) field selects from r.
func regexFieldStrings(r Record, field RegexField) []string {
	switch field {
	case FieldValue:
		return []string{r.Value}
	case FieldKey:
		return []string{r.Key}
	case FieldHeaders:
		out := make([]string, len(r.Headers))
		for i, h := range r.Headers {
			out[i] = h.Value
		}
		return out
	default:
		return nil
	}
}

// regexMatch runs re against s, bounded by timeout when positive.
func regexMatch(re *regexp.Regexp, s string, timeout time.Duration) (matched, timedOut bool) {
	return runWithTimeout(timeout, func() bool { return re.MatchString(s) })
}

// runWithTimeout runs work in its own goroutine, bounded by timeout when
// positive. A non-positive timeout runs work synchronously with no bound.
// Factored out from regexMatch so the timeout path itself is deterministically
// testable with an injected slow work func, independent of how fast any real
// regex actually runs (TECH-SPEC §4.5 Regex/JSONPath).
func runWithTimeout(timeout time.Duration, work func() bool) (result, timedOut bool) {
	if timeout <= 0 {
		return work(), false
	}
	done := make(chan bool, 1)
	go func() { done <- work() }()
	select {
	case r := <-done:
		return r, false
	case <-time.After(timeout):
		return false, true
	}
}
