package authorization

func roleLevel(role Role) (int, error) {
	switch role {
	case RoleViewer:
		return 1, nil
	case RoleOperator:
		return 2, nil
	case RoleAdmin:
		return 3, nil
	default:
		return 0, ErrInvalidRole
	}
}
