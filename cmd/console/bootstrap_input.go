package main

import "errors"

const maxBootstrapInputBytes = 128 + 1 + 64 + 1 + 1024

var errInvalidBootstrapInput = errors.New("invalid bootstrap input")

type bootstrapInput struct {
	secret   string
	username string
	password string
}
