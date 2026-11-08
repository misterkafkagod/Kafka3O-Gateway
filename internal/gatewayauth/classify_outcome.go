package gatewayauth

import (
	"encoding/json"
	"strings"
)

// classifyOutcome maps a mutation handler's response to the outcome recorded for its intent. Acceptance is not
// completion: any "*_unknown" problem is recorded as unknown, never as success or failure.
func classifyOutcome(status int, body []byte) string {
	switch {
	case status >= 200 && status < 300:
		return "confirmed"
	case status >= 400 && status < 500:
		return "validation_rejected"
	}
	var problem struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &problem) == nil && strings.HasSuffix(problem.Code, "_unknown") {
		return "unknown"
	}
	if status >= 500 {
		return "failed"
	}
	return "unknown"
}
