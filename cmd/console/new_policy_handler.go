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

// newPolicyHandler wires the delegated-authorization decision; protector supplies the Cognito refresh-token
// protector once discovery has published it.
func newPolicyHandler(db *storage.Database, protector delegation.ProtectorSource, logger *slog.Logger) (http.Handler, error) {
	consume, err := delegation.NewConsumeDelegation(db, db, db, protector, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	return policyapi.NewHandler(consume, db, logger)
}
