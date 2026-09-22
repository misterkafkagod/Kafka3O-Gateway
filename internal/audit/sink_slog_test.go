package audit_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
)

func TestSlogSink_NeverLogsPasswordField(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	sink := audit.NewSlogSink(slog.New(slog.NewJSONHandler(&buf, nil)))

	keyID := "k1"
	err := sink.Write(context.Background(), audit.Event{
		EventID: "e1", CommandID: "M5",
		Caller:     audit.Caller{KeyID: &keyID, Tier: "operator", ClientIP: "127.0.0.1"},
		BreakGlass: &audit.BreakGlass{Reason: "incident-42"},
		Outcome:    audit.OutcomeSucceeded,
	})
	if err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	if strings.Contains(strings.ToLower(buf.String()), "password") {
		t.Errorf("log output contains \"password\" (FUNC-SPEC §8.2; TECH-SPEC C7): %s", buf.String())
	}
}
