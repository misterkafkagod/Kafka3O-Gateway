package storage

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDatabaseLockEnforcesOneOwnerAndReleasesOnClose(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	first, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenDatabase(ctx, path)
	if second != nil || !errors.Is(err, ErrDatabaseInUse) {
		t.Fatalf("second database owner: db=%v err=%v", second, err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestDatabaseLockHelper$")
	child.Env = append(os.Environ(), "KAFKA3O_TEST_LOCK_PATH="+path, "KAFKA3O_TEST_LOCK_MODE=busy")
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("another process did not observe the database lock: %v; output=%s", err, output)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("database close is not idempotent: %v", err)
	}
	child = exec.Command(os.Args[0], "-test.run=^TestDatabaseLockHelper$")
	child.Env = append(os.Environ(), "KAFKA3O_TEST_LOCK_PATH="+path, "KAFKA3O_TEST_LOCK_MODE=exit")
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("subprocess could not acquire the database: %v; output=%s", err, output)
	}
	afterExit, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatalf("lock was not released when its process exited: %v", err)
	}
	if err := afterExit.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseLockHelper(t *testing.T) {
	path, mode := os.Getenv("KAFKA3O_TEST_LOCK_PATH"), os.Getenv("KAFKA3O_TEST_LOCK_MODE")
	if path == "" {
		return
	}
	db, err := OpenDatabase(context.Background(), path)
	switch mode {
	case "busy":
		if db != nil || !errors.Is(err, ErrDatabaseInUse) {
			t.Fatalf("expected the database to be in use: %v", err)
		}
	case "exit":
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	default:
		t.Fatal("invalid lock-test mode")
	}
}

func TestDatabaseLockRejectsInsecureOrSymlinkedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "console.sqlite")
	lockPath := path + ".lock"
	if runtime.GOOS != "windows" {
		if err := os.WriteFile(lockPath, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(lockPath, 0644); err != nil {
			t.Fatal(err)
		}
		db, err := OpenDatabase(context.Background(), path)
		if db != nil || !errors.Is(err, ErrInvalidDatabaseLock) {
			t.Fatalf("insecure lock file: db=%v err=%v", db, err)
		}
		if err := os.Remove(lockPath); err != nil {
			t.Fatal(err)
		}
	}
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("creating a symlink is unavailable: %v", err)
		}
		t.Fatal(err)
	}
	db, err := OpenDatabase(context.Background(), path)
	if db != nil || !errors.Is(err, ErrInvalidDatabaseLock) {
		t.Fatalf("symlinked lock file: db=%v err=%v", db, err)
	}
	contents, err := os.ReadFile(target)
	if err != nil || string(contents) != "unchanged" {
		t.Fatalf("lock symlink target was changed: %v", err)
	}
}
