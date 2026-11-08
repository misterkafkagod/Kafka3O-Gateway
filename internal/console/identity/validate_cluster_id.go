package identity

func ValidateClusterID(id string) error {
	if !validClusterID(id) {
		return ErrInvalidClusterID
	}
	return nil
}
