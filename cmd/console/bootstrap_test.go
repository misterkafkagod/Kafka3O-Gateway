package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func TestBootstrapCommandUsesPipeAndCreatesOneAdministrator(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	const username, password = "Admin", "initial passphrase"
	env := func(key string) string {
		switch key {
		case "KAFKA3O_CONSOLE_DB_PATH":
			return path
		case "KAFKA3O_CONSOLE_BOOTSTRAP_SECRET":
			return secret
		default:
			return ""
		}
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = io.WriteString(write, secret+"\n"+username+"\n"+password)
		_ = write.Close()
	}()
	err = run(ctx, []string{"bootstrap"}, read, env)
	_ = read.Close()
	if err != nil {
		t.Fatalf("bootstrap command: %v", err)
	}
	if err := runBootstrap(ctx, path, secret, strings.NewReader(secret+"\nother\nanother passphrase")); !errors.Is(err, identity.ErrAlreadyBootstrapped) {
		t.Fatalf("reused bootstrap: %v", err)
	}
	db, err := storage.OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	account, err := db.FindLocalAccount(ctx, username)
	if err != nil || !account.Enabled || !account.MustChangePassword {
		t.Fatalf("initial account was not persisted: %v", err)
	}
	matches, err := identity.VerifyLocalPassword(password, account.PasswordHash)
	if err != nil || !matches {
		t.Fatalf("initial credential was not stored as a valid hash: %v", err)
	}
	if _, err := db.FindLocalAccount(ctx, "other"); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("second account unexpectedly exists: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(disk, []byte(secret)) || bytes.Contains(disk, []byte(password)) {
		t.Fatal("plaintext bootstrap credentials were persisted")
	}
}

func TestBootstrapCommandRejectsFilesAndInvalidUsage(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "input")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	env := func(string) string { return "" }
	if err := run(context.Background(), []string{"bootstrap"}, file, env); !errors.Is(err, errBootstrapInputPipe) {
		t.Fatalf("regular-file input: %v", err)
	}
	if err := run(context.Background(), []string{"bootstrap", "extra"}, file, env); !errors.Is(err, errConsoleUsage) {
		t.Fatalf("arguments: %v", err)
	}
}

func TestBootstrapCommandAuditsMalformedInputAndRejectsBadProof(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))
	if err := runBootstrap(ctx, path, secret, strings.NewReader("malformed")); !errors.Is(err, errInvalidBootstrapInput) {
		t.Fatalf("malformed input: %v", err)
	}
	if err := runBootstrap(ctx, path, secret, strings.NewReader("wrong-proof\nadmin\ninitial passphrase")); !errors.Is(err, identity.ErrInvalidBootstrapSecret) {
		t.Fatalf("incorrect proof: %v", err)
	}
	invalidUTF8 := append([]byte(secret+"\nadmin\ninitial passphrase"), 0xff)
	if err := runBootstrap(ctx, path, secret, bytes.NewReader(invalidUTF8)); !errors.Is(err, identity.ErrInvalidPassword) {
		t.Fatalf("invalid UTF-8 password: %v", err)
	}
	db, err := storage.OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	events, err := db.ListAuditEvents(ctx, storage.AuditFilter{Global: true, Action: "console.bootstrap", Limit: 10})
	if err != nil || len(events) != 6 {
		t.Fatalf("bootstrap audit event count=%d err=%v", len(events), err)
	}
	for i := 0; i < len(events); i += 2 {
		if events[i].Outcome != storage.AuditAttempted || events[i+1].Outcome != storage.AuditFailed ||
			events[i].EventID != events[i+1].OperationID || events[i+1].ActorID != "anonymous" {
			t.Fatalf("malformed bootstrap attempt %d was not audited", i/2)
		}
	}
	if _, err := db.FindLocalAccount(ctx, "admin"); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("rejected input created an account: %v", err)
	}
}

func TestReadBootstrapInputPreservesPasswordAndBounds(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	password := "line one\nline two"
	input, err := readBootstrapInput(strings.NewReader(secret + "\nadmin\n" + password))
	if err != nil || input.secret != secret || input.username != "admin" || input.password != password {
		t.Fatal("bootstrap input was not preserved")
	}
	for _, invalid := range []string{
		"", "one\ntwo", strings.Repeat("x", 129) + "\nadmin\npass",
		secret + "\n" + strings.Repeat("x", 65) + "\npass",
		secret + "\nadmin\n" + strings.Repeat("p", 1025),
	} {
		if _, err := readBootstrapInput(strings.NewReader(invalid)); !errors.Is(err, errInvalidBootstrapInput) {
			t.Fatalf("invalid input was accepted: %v", err)
		}
	}
}

func TestBootstrapCommandRejectsInvalidConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console.sqlite")
	if err := runBootstrap(context.Background(), path, "short", strings.NewReader("")); !errors.Is(err, identity.ErrInvalidBootstrapConfig) {
		t.Fatalf("weak configured bootstrap secret: %v", err)
	}

	if err := runBootstrap(context.Background(), "relative.sqlite", base64.RawURLEncoding.EncodeToString(make([]byte, 32)), rand.Reader); !errors.Is(err, storage.ErrInvalidDatabasePath) {
		t.Fatalf("relative database path: %v", err)
	}
}

func TestBootstrapCommandProcessKeepsCredentialsOutOfOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console.sqlite")
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	const password = "initial passphrase"
	cmd := exec.Command(os.Args[0], "-test.run=^TestBootstrapHelperProcess$")
	cmd.Env = append(os.Environ(),
		"KAFKA3O_TEST_BOOTSTRAP_HELPER=1",
		"KAFKA3O_CONSOLE_DB_PATH="+path,
		"KAFKA3O_CONSOLE_BOOTSTRAP_SECRET="+secret,
	)
	cmd.Stdin = strings.NewReader(secret + "\nadmin\n" + password)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("bootstrap process: %v; stderr=%s", err, stderr.String())
	}
	output := stdout.String() + stderr.String()
	if !strings.Contains(output, "Initial administrator created") ||
		strings.Contains(output, secret) || strings.Contains(output, password) {
		t.Fatal("bootstrap process output was incorrect or contained credentials")
	}
}

func TestBootstrapHelperProcess(t *testing.T) {
	if os.Getenv("KAFKA3O_TEST_BOOTSTRAP_HELPER") != "1" {
		return
	}
	os.Args = []string{"console", "bootstrap"}
	main()
}
