package telemetry_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/telemetry"
)

func TestTelemetry_LogLinesCarryRequestID(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf, "test-service")

	logger.Info("handled request", "requestId", "req-123", "status", 200)

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line is not valid JSON: %v (line: %s)", err, buf.String())
	}

	if line["msg"] != "handled request" {
		t.Errorf("msg = %v, want %q", line["msg"], "handled request")
	}
	if line["requestId"] != "req-123" {
		t.Errorf("requestId = %v, want req-123", line["requestId"])
	}
}
