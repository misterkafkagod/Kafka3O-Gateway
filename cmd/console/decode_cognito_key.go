package main

import "encoding/base64"

func decodeCognitoKey(value string) ([]byte, bool) {
	key, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(key) != 32 || base64.RawURLEncoding.EncodeToString(key) != value {
		return nil, false
	}
	var nonzero byte
	for _, b := range key {
		nonzero |= b
	}
	return key, nonzero != 0
}
