package authorization

// ResolveRole combines global grants with grants for the named cluster.
// An empty cluster ID resolves global grants only.
func ResolveRole(grants []Grant, clusterID string) (Role, error) {
	var role Role
	best := 0
	for _, grant := range grants {
		level, err := roleLevel(grant.Role)
		if err != nil {
			return RoleNone, err
		}
		if grant.ClusterID != "" && (clusterID == "" || grant.ClusterID != clusterID) {
			continue
		}
		if level > best {
			role, best = grant.Role, level
		}
	}
	return role, nil
}
