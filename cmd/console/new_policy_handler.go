package main

import (
	"crypto/rand"
	"log/slog"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/policyapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

// newPolicyHandler wires the delegated-authorization decision. Cognito sessions fail closed here until the
// refresh-token protector is wired for delegated requests.
func newPolicyHandler(db *storage.Database, logger *slog.Logger) (http.Handler, error) {
	consume, err := delegation.NewConsumeDelegation(db, db, db, nil, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	return policyapi.NewHandler(consume, db, logger)
}
