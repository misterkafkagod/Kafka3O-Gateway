package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) ReserveBoundCognitoRefresh(ctx context.Context, request cognito.RefreshReservationRequest, browserHash [32]byte) (cognito.RefreshReservation, error) {
	if browserHash == ([32]byte{}) {
		return cognito.RefreshReservation{}, cognito.ErrRefreshUnavailable
	}
	return d.reserveCognitoRefreshWithBrowserBinding(ctx, request, browserHash)
}
