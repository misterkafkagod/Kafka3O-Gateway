package main

import (
	"context"
	"errors"
	"os"
)

var (
	errConsoleUsage       = errors.New("usage: console bootstrap")
	errBootstrapInputPipe = errors.New("bootstrap input must come from a pipe, not a terminal or file")
)

func run(ctx context.Context, args []string, input *os.File, getenv func(string) string) error {
	if len(args) != 1 || args[0] != "bootstrap" {
		return errConsoleUsage
	}
	info, err := input.Stat()
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		return errBootstrapInputPipe
	}
	return runBootstrap(ctx, getenv("KAFKA3O_CONSOLE_DB_PATH"), getenv("KAFKA3O_CONSOLE_BOOTSTRAP_SECRET"), input)
}
