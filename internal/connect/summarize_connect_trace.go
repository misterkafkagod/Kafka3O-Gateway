package connect

import "strings"

const maxTraceSummary = 500

// SummarizeConnectTrace keeps the first line of a Java stack trace, which states the failure, and caps its length.
func SummarizeConnectTrace(trace string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(trace), "\n")
	line = strings.TrimSpace(line)
	if runes := []rune(line); len(runes) > maxTraceSummary {
		return string(runes[:maxTraceSummary]) + "…"
	}
	return line
}
