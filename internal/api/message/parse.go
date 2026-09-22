package message

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

// ParseFrom parses one `from=`/`to=` value — beginning, latest, offset:<n>,
// or timestamp:<ms|iso> — into a message.From (FUNC-SPEC §8.7 M1; TASKS.md
// Task 3.4.1). An empty string is invalid: From is a required parameter,
// but To reuses this parser too, where empty means "not supplied" and is
// handled by the caller before ParseFrom is ever called.
func ParseFrom(s string) (message.From, error) {
	switch {
	case s == "beginning":
		return message.From{Kind: message.FromBeginning}, nil
	case s == "latest":
		return message.From{Kind: message.FromLatest}, nil
	case strings.HasPrefix(s, "offset:"):
		n, err := strconv.ParseInt(strings.TrimPrefix(s, "offset:"), 10, 64)
		if err != nil {
			return message.From{}, fmt.Errorf("invalid from=offset:%s", strings.TrimPrefix(s, "offset:"))
		}
		return message.From{Kind: message.FromOffset, Offset: n}, nil
	case strings.HasPrefix(s, "timestamp:"):
		raw := strings.TrimPrefix(s, "timestamp:")
		ms, err := parseTimestamp(raw)
		if err != nil {
			return message.From{}, fmt.Errorf("invalid from=timestamp:%s", raw)
		}
		return message.From{Kind: message.FromTimestamp, TimeMilli: ms}, nil
	default:
		return message.From{}, fmt.Errorf("invalid from: %q", s)
	}
}

// parseTimestamp accepts either epoch milliseconds or an RFC3339 (ISO-8601)
// timestamp (FUNC-SPEC §8.7 M1 "timestamp:<ms|iso>").
func parseTimestamp(raw string) (int64, error) {
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return ms, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp %q: neither epoch milliseconds nor RFC3339", raw)
	}
	return t.UnixMilli(), nil
}

// parseRegexFields converts M3's `fields` strings into scan.RegexField
// values. An empty list is left as nil — NewRegexMatcher defaults it to
// []{FieldValue} (FUNC-SPEC §8.7 M3).
func parseRegexFields(raw []string) ([]scan.RegexField, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]scan.RegexField, len(raw))
	for i, s := range raw {
		switch s {
		case "value":
			out[i] = scan.FieldValue
		case "key":
			out[i] = scan.FieldKey
		case "headers":
			out[i] = scan.FieldHeaders
		default:
			return nil, fmt.Errorf("invalid fields entry %q: want value, key, or headers", s)
		}
	}
	return out, nil
}

// filterOps maps M4's wire op names onto scan.FilterOp (FUNC-SPEC V4).
var filterOps = map[string]scan.FilterOp{
	"eq": scan.OpEq, "neq": scan.OpNeq, "contains": scan.OpContains,
	"regex": scan.OpRegex, "exists": scan.OpExists,
	"gt": scan.OpGt, "lt": scan.OpLt, "gte": scan.OpGte, "lte": scan.OpLte,
}

// parseFilterOp validates and converts one M4 `op` string.
func parseFilterOp(s string) (scan.FilterOp, error) {
	op, ok := filterOps[s]
	if !ok {
		return "", fmt.Errorf("invalid op %q: want one of eq, neq, contains, regex, exists, gt, lt, gte, lte", s)
	}
	return op, nil
}
