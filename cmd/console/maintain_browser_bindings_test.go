package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

type browserCleanupWorkerFixture struct {
	cleanupWorkerFixture
}

func (f *browserCleanupWorkerFixture) CleanupBrowserBindings(ctx context.Context, operation string, at time.Time) (int, error) {
	return f.CleanupCognitoCredentials(ctx, operation, at)
}

type blockingBrowserCleanupWorker struct {
	started chan struct{}
	unknown bool
}

func (f *blockingBrowserCleanupWorker) CleanupBrowserBindings(ctx context.Context, _ string, _ time.Time) (int, error) {
	close(f.started)
	<-ctx.Done()
	if f.unknown {
		return 0, errors.Join(storage.ErrBrowserBindingCleanupUnknown, ctx.Err())
	}
	return 0, ctx.Err()
}

func TestBrowserBindingCleanupWorkerSchedulesAndReportsFailures(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	fixture := &browserCleanupWorkerFixture{cleanupWorkerFixture{
		calls: make(chan cleanupWorkerCall, 2),
		err:   errors.New("private browser marker and credential"),
		count: storage.BrowserBindingCleanupBatchSize,
	}}
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	at := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	random := bytes.NewReader(append(bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 16)...))
	started := time.Now()
	go func() {
		defer close(done)
		maintainBrowserBindings(ctx, fixture, random, func() time.Time { return at }, ticks, logger)
	}()
	first := <-fixture.calls
	if first.at != at || first.deadline.Before(started.Add(9*time.Second)) || first.deadline.After(started.Add(11*time.Second)) {
		t.Fatal("startup browser cleanup did not use the bounded deadline and clock")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(first.operation)
	if err != nil || len(decoded) != 16 {
		t.Fatal("browser cleanup operation identifier did not contain 128 random bits")
	}
	select {
	case <-fixture.calls:
		t.Fatal("browser cleanup was retried without a scheduler tick")
	case <-time.After(25 * time.Millisecond):
	}
	ticks <- at.Add(time.Minute)
	second := <-fixture.calls
	if second.operation == first.operation {
		t.Fatal("scheduled browser cleanup reused its operation identifier")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("browser cleanup worker did not stop")
	}
	if !strings.Contains(logs.String(), "Browser binding cleanup unavailable") ||
		!strings.Contains(logs.String(), "Browser binding cleanup batch limit reached") ||
		!strings.Contains(logs.String(), "Inactive browser bindings removed") ||
		strings.Contains(logs.String(), "private browser marker and credential") {
		t.Fatal("browser cleanup did not report failures and batch limits safely")
	}
}

func TestBrowserBindingCleanupWorkerReportsUnknownShutdown(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown=%t", unknown), func(t *testing.T) {
			fixture := &blockingBrowserCleanupWorker{started: make(chan struct{}), unknown: unknown}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			go func() {
				defer close(done)
				maintainBrowserBindings(ctx, fixture, bytes.NewReader(make([]byte, 16)), time.Now, make(chan time.Time), logger)
			}()
			select {
			case <-fixture.started:
			case <-time.After(5 * time.Second):
				t.Fatal("browser cleanup worker did not start")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("browser cleanup worker did not cancel its work")
			}
			if unknown && !strings.Contains(logs.String(), "level=ERROR") {
				t.Fatal("unknown browser cleanup outcome was hidden during shutdown")
			}
			if !unknown && !strings.Contains(logs.String(), "interrupted by shutdown") {
				t.Fatal("browser cleanup shutdown interruption was not reported")
			}
		})
	}
}
