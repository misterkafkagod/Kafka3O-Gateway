package main

import (
	"bytes"
	"fmt"
	"io"
)

func readBootstrapInput(input io.Reader) (bootstrapInput, error) {
	data, err := io.ReadAll(io.LimitReader(input, maxBootstrapInputBytes+1))
	if err != nil {
		return bootstrapInput{}, fmt.Errorf("read bootstrap input: %w", err)
	}
	defer clear(data)
	if len(data) > maxBootstrapInputBytes {
		return bootstrapInput{}, errInvalidBootstrapInput
	}
	parts := bytes.SplitN(data, []byte{'\n'}, 3)
	if len(parts) != 3 || len(parts[0]) > 128 || len(parts[1]) > 64 || len(parts[2]) > 1024 {
		return bootstrapInput{}, errInvalidBootstrapInput
	}
	return bootstrapInput{secret: string(parts[0]), username: string(parts[1]), password: string(parts[2])}, nil
}
