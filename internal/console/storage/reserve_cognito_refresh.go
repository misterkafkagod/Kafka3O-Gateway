package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) ReserveCognitoRefresh(ctx context.Context, request cognito.RefreshReservationRequest) (cognito.RefreshReservation, error) {
	return d.reserveCognitoRefreshWithBrowserBinding(ctx, request, [32]byte{})
}
