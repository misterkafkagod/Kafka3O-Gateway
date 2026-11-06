package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "console:", err)
		os.Exit(1)
	}
	if _, err := fmt.Fprintln(os.Stdout, "Initial administrator created. Sign in and replace the temporary password."); err != nil {
		fmt.Fprintln(os.Stderr, "console: bootstrap completed, but writing confirmation failed:", err)
		os.Exit(1)
	}
}
