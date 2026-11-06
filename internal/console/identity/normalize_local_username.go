package identity

import "strings"

func NormalizeLocalUsername(username string) (string, error) {
	if len(username) < 1 || len(username) > 64 {
		return "", ErrInvalidUsername
	}
	for i := 0; i < len(username); i++ {
		character := username[i]
		letter := character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z'
		digit := character >= '0' && character <= '9'
		if !letter && !digit && (i == 0 || character != '.' && character != '_' && character != '-') {
			return "", ErrInvalidUsername
		}
	}
	return strings.ToLower(username), nil
}
