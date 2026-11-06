package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

type cleanupWorkerCall struct {
	operation string
	at        time.Time
	deadline  time.Time
}

type cleanupWorkerFixture struct {
	calls chan cleanupWorkerCall
	err   error
	count int
}

func (f *cleanupWorkerFixture) CleanupCognitoCredentials(ctx context.Context, operation string, at time.Time) (int, error) {
	deadline, _ := ctx.Deadline()
	f.calls <- cleanupWorkerCall{operation: operation, at: at, deadline: deadline}
	err := f.err
	f.err = nil
	return f.count, err
}

type blockingCleanupWorker struct {
	started chan struct{}
	unknown bool
}

func (f *blockingCleanupWorker) CleanupCognitoCredentials(ctx context.Context, _ string, _ time.Time) (int, error) {
	close(f.started)
	<-ctx.Done()
	if f.unknown {
		return 0, errors.Join(storage.ErrCognitoCleanupUnknown, ctx.Err())
	}
	return 0, ctx.Err()
}

func TestCognitoCredentialCleanupWorkerSchedulesAndReportsFailures(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	fixture := &cleanupWorkerFixture{
		calls: make(chan cleanupWorkerCall, 2),
		err:   errors.New("private cleanup detail"),
		count: storage.CognitoCleanupBatchSize,
	}
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	at := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	random := bytes.NewReader(append(bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 16)...))
	started := time.Now()
	go func() {
		defer close(done)
		maintainCognitoCredentials(ctx, fixture, random, func() time.Time { return at }, ticks, logger)
	}()
	first := <-fixture.calls
	if first.at != at || first.deadline.Before(started.Add(9*time.Second)) || first.deadline.After(started.Add(11*time.Second)) {
		t.Fatal("startup cleanup did not use the bounded deadline and clock")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(first.operation)
	if err != nil || len(decoded) != 16 {
		t.Fatal("cleanup operation identifier was not generated from 128 random bits")
	}
	select {
	case <-fixture.calls:
		t.Fatal("cleanup failure was retried without a scheduler tick")
	case <-time.After(25 * time.Millisecond):
	}
	ticks <- at.Add(time.Minute)
	second := <-fixture.calls
	if second.operation == first.operation {
		t.Fatal("scheduled cleanup reused an operation identifier")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup worker did not stop on shutdown")
	}
	if !strings.Contains(logs.String(), "Cognito credential cleanup unavailable") ||
		!strings.Contains(logs.String(), "Cognito credential cleanup batch limit reached") ||
		strings.Contains(logs.String(), "private cleanup detail") {
		t.Fatal("cleanup failure was not reported safely")
	}
}

func TestCognitoCredentialCleanupWorkerCancelsInFlightWork(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown=%t", unknown), func(t *testing.T) {
			fixture := &blockingCleanupWorker{started: make(chan struct{}), unknown: unknown}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			go func() {
				defer close(done)
				maintainCognitoCredentials(ctx, fixture, bytes.NewReader(make([]byte, 16)), time.Now, make(chan time.Time), logger)
			}()
			select {
			case <-fixture.started:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup worker did not start")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup worker did not cancel in-flight work")
			}
			if unknown && !strings.Contains(logs.String(), "level=ERROR") {
				t.Fatal("unknown cleanup outcome was hidden during shutdown")
			}
			if !unknown && !strings.Contains(logs.String(), "interrupted by shutdown") {
				t.Fatal("cleanup shutdown interruption was not reported")
			}
		})
	}
}

func TestCognitoCredentialCleanupWorkerReportsEntropyAndSchedulerFailures(t *testing.T) {
	for _, mode := range []string{"entropy", "scheduler"} {
		t.Run(mode, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			fixture := &cleanupWorkerFixture{calls: make(chan cleanupWorkerCall, 1)}
			ticks := make(chan time.Time)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var random io.Reader = bytes.NewReader(make([]byte, 16))
			if mode == "entropy" {
				random = bytes.NewReader(nil)
			} else {
				close(ticks)
			}
			go func() {
				defer close(done)
				maintainCognitoCredentials(ctx, fixture, random, time.Now, ticks, logger)
			}()
			if mode == "entropy" {
				close(ticks)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				cancel()
				t.Fatal("cleanup worker did not report stopped scheduler")
			}
			cancel()
			if !strings.Contains(logs.String(), "Cognito credential cleanup scheduler stopped") {
				t.Fatal("stopped cleanup scheduler was not reported")
			}
			if mode == "entropy" {
				if len(fixture.calls) != 0 || !strings.Contains(logs.String(), "Cognito credential cleanup unavailable") {
					t.Fatal("entropy failure was hidden or dispatched cleanup")
				}
			} else if len(fixture.calls) != 1 {
				t.Fatal("startup cleanup did not run before scheduler stopped")
			}
		})
	}
}
