// Command gateway is the Kafka3O Gateway entrypoint (TECH-SPEC §5.1, §6.1
// B3/B4): `serve` (default) runs the gateway, `probe` and `keygen` are
// small self-contained CLI utilities exec'd by Kubernetes probes and
// operators. It is a thin shell around internal/app and internal/config
// (TECH-SPEC §5.3) — depguard enforces that it imports nothing else.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/misterkafkagod/kafka3o/internal/app"
	"github.com/misterkafkagod/kafka3o/internal/config"
)

// version is injected at build time: -ldflags "-X main.version=<tag>" (see Makefile).
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Environ()))
}

// run dispatches to the four subcommands (TECH-SPEC §6.1 B3/B4): `serve`
// is the default when no recognised subcommand is given, so `--config` at
// the top level still works.
func run(args, environ []string) int {
	if len(args) > 0 {
		switch args[0] {
		case "--version":
			fmt.Println(version)
			return 0
		case "probe":
			return runProbe(args[1:], environ)
		case "keygen":
			return runKeygen(args[1:])
		case "serve":
			args = args[1:]
		}
	}
	return runServe(args, environ)
}

// runServe loads the configuration, then runs the gateway until SIGTERM or
// SIGINT (TECH-SPEC §5.4).
func runServe(args, environ []string) int {
	path := config.PathFromArgs(args, environ)
	cfg, err := config.Load(config.Options{Path: path, Environ: environ})
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o-gateway:", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o-gateway:", err)
		return 1
	}
	return 0
}
