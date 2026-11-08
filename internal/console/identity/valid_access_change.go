package identity

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"

// validAccessChange checks the scope and the expected/new roles of an access change.
func validAccessChange(clusterID string, expected, role authorization.Role) bool {
	if clusterID != "" && !validClusterID(clusterID) {
		return false
	}
	for _, value := range []authorization.Role{expected, role} {
		switch value {
		case authorization.RoleNone, authorization.RoleViewer, authorization.RoleOperator, authorization.RoleAdmin:
		default:
			return false
		}
	}
	return true
}
