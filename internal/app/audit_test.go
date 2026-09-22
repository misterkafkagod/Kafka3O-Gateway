package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
)

func TestApp_AuditTopicMissing_LogsErrorAndMarksSinkUnhealthy(t *testing.T) {
	t.Parallel()
	f := fake.New() // the audit topic is never seeded
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	healthy := auditHealthAfterVerify(context.Background(), f, f, "_kgw_audit", logger)

	if healthy {
		t.Error("auditHealthAfterVerify() = true, want false (the audit topic does not exist)")
	}
	if !strings.Contains(buf.String(), `"level":"ERROR"`) {
		t.Errorf("log output missing an ERROR line: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "_kgw_audit") {
		t.Errorf("log output missing the audit topic name: %s", buf.String())
	}
}

func TestApp_AuditTopicNeverAutoCreated(t *testing.T) {
	t.Parallel()
	f := fake.New() // the audit topic is never seeded
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	auditHealthAfterVerify(context.Background(), f, f, "_kgw_audit", logger)

	// kafka.Admin has no CreateTopics method yet (Phase 2 built inspection
	// only), so the strongest available proof that verification never
	// auto-creates the topic is that it never reached a mutating call at
	// all: DescribeTopics on a missing topic fails closed before the probe
	// Produce that would otherwise be the only mutating call in play.
	if calls := f.MutatingCalls(); len(calls) != 0 {
		t.Errorf("MutatingCalls() = %+v, want none", calls)
	}
}

func TestApp_AuditTopicPresentAndWritable_IsHealthy(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("_kgw_audit", 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	healthy := auditHealthAfterVerify(context.Background(), f, f, "_kgw_audit", logger)

	if !healthy {
		t.Error("auditHealthAfterVerify() = false, want true (topic exists and accepts a write)")
	}
}
