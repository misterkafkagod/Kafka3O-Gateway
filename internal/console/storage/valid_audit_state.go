package storage

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"

func validAuditState(state string) bool {
	switch authorization.Role(state) {
	case authorization.RoleNone, authorization.RoleViewer, authorization.RoleOperator, authorization.RoleAdmin:
		return true
	}
	switch authorization.Environment(state) {
	case authorization.EnvironmentNonProduction, authorization.EnvironmentProduction:
		return true
	}
	return false
}
