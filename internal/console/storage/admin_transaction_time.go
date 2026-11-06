package storage

import (
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) adminTransactionTime(actor identity.LocalAdminActor, requested time.Time) (time.Time, error) {
	if actor.BrowserBindingHash != ([32]byte{}) {
		return d.strictTransactionTime(requested)
	}
	return d.transactionTime(requested)
}
