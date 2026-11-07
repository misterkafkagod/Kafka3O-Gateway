package storage

import (
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func cognitoSessionExchangeDetails(record session.CognitoRecord, now time.Time) (cognito.SessionExchangeDetails, error) {
	if record.Session.BrowserBindingHash == ([32]byte{}) {
		return record.Exchange.Details(record.Protector, now)
	}
	return record.Exchange.BoundDetails(record.Protector, now, record.Session.BrowserBindingHash)
}
