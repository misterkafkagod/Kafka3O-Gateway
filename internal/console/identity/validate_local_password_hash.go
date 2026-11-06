package identity

func ValidateLocalPasswordHash(encoded string) error {
	_, err := parseLocalPasswordHash(encoded)
	return err
}
