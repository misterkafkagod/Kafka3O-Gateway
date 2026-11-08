package main

import (
	"crypto/rand"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/gatewayproxy"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

// enableGatewayRequests wires same-origin forwarding to secured Gateways with the Console's client identity.
func enableGatewayRequests(handler *consoleapi.SessionAPI, db *storage.Database, config forwardingConfig) error {
	mint, err := delegation.NewMintDelegation(db, rand.Reader, time.Now)
	if err != nil {
		return err
	}
	forwarder, err := gatewayproxy.NewForwardRequest(mint, config.identity, config.serviceCA)
	if err != nil {
		return err
	}
	return handler.EnableGatewayRequests(forwarder)
}
