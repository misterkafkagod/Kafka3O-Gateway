package scan

// MatchResult is one decoded record's outcome against a Matcher (FUNC-SPEC
// §9.2 Evaluate). Neither field set means the record was evaluated but is
// simply not a match (M1 never produces this — every record matches).
type MatchResult struct {
	Match   bool
	Skipped bool
}

// Matcher decides whether one decoded record matches, is skipped (an
// undecodable value, a JSON-parse failure, or a regex timeout — never an
// error, FUNC-SPEC §9.2), or neither (TECH-SPEC I4: a func type, not an
// interface).
type Matcher func(Record) MatchResult

// MatchAll is M1's matcher: every record matches, nothing is ever skipped
// (FUNC-SPEC §9.2: "M1 has no filter").
func MatchAll(Record) MatchResult { return MatchResult{Match: true} }
