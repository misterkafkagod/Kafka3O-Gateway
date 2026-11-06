package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
)

var (
	errConsoleUsage       = errors.New("usage: console bootstrap|serve")
	errBootstrapInputPipe = errors.New("bootstrap input must come from a pipe, not a terminal or file")
	errBootstrapSecretSet = errors.New("remove the bootstrap secret from the environment before serving")
)

func run(ctx context.Context, args []string, input *os.File, getenv func(string) string) error {
	if len(args) != 1 {
		return errConsoleUsage
	}
	switch args[0] {
	case "bootstrap":
		info, err := input.Stat()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeNamedPipe == 0 {
			return errBootstrapInputPipe
		}
		return runBootstrap(ctx, getenv("KAFKA3O_CONSOLE_DB_PATH"), getenv("KAFKA3O_CONSOLE_BOOTSTRAP_SECRET"), input)
	case "serve":
		if getenv("KAFKA3O_CONSOLE_BOOTSTRAP_SECRET") != "" {
			return errBootstrapSecretSet
		}
		logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
		return runServe(ctx, getenv, net.Listen, logger)
	default:
		return errConsoleUsage
	}
}
