package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func runBootstrap(ctx context.Context, databasePath, expectedSecret string, input io.Reader) (result error) {
	db, err := storage.OpenDatabase(ctx, databasePath)
	if err != nil {
		return fmt.Errorf("open Console store: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close Console store; inspect bootstrap state before retrying: %w", err))
		}
	}()
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, expectedSecret, rand.Reader, time.Now)
	if err != nil {
		return fmt.Errorf("configure bootstrap: %w", err)
	}
	request, err := readBootstrapInput(input)
	if err != nil {
		_, auditErr := bootstrap.Execute(ctx, "", "", "")
		if auditErr == nil {
			return errors.Join(err, errors.New("malformed bootstrap input unexpectedly succeeded"))
		}
		if auditErr != identity.ErrInvalidBootstrapSecret {
			return errors.Join(err, fmt.Errorf("bootstrap rejection or audit failure: %w", auditErr))
		}
		return err
	}
	if _, err := bootstrap.Execute(ctx, request.secret, request.username, request.password); err != nil {
		return fmt.Errorf("bootstrap did not complete: %w", err)
	}
	return nil
}
