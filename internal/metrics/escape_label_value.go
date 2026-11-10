package metrics

import "strings"

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// EscapeLabelValue escapes a Prometheus label value (backslash, double quote, line feed).
func EscapeLabelValue(value string) string {
	return labelEscaper.Replace(value)
}
