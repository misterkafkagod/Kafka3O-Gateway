package authorization

import "strings"

func EvaluatePolicy(request PolicyRequest) (Decision, error) {
	category, err := classifyAction(request.Action)
	if err != nil {
		return Decision{}, err
	}
	clusterID := request.ClusterID
	if category == globalAdministration {
		clusterID = ""
	} else if strings.TrimSpace(clusterID) == "" {
		return Decision{}, ErrMissingCluster
	}

	role, err := ResolveRole(request.Grants, clusterID)
	if err != nil {
		return Decision{}, err
	}
	decision := Decision{EffectiveRole: role}
	if role == RoleNone {
		decision.Denial = DenialInsufficientRole
		return decision, nil
	}

	switch category {
	case globalAdministration, clusterAuditRead:
		decision.Allowed = role == RoleAdmin
	case clusterRead:
		decision.Allowed = true
	case kafkaWrite:
		if role == RoleViewer {
			decision.Denial = DenialInsufficientRole
			return decision, nil
		}
		switch request.Environment {
		case EnvironmentUnclassified:
			decision.Denial = DenialUnclassified
			return decision, nil
		case EnvironmentNonProduction:
			decision.Allowed = true
		case EnvironmentProduction:
			if role != RoleAdmin {
				decision.Denial = DenialProductionAdmin
				return decision, nil
			}
			if strings.TrimSpace(request.ChangeReason) == "" {
				decision.Denial = DenialReasonRequired
				return decision, nil
			}
			decision.Allowed = true
		default:
			return Decision{}, ErrInvalidEnvironment
		}
	}
	if !decision.Allowed {
		decision.Denial = DenialInsufficientRole
	}
	return decision, nil
}
