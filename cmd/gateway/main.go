// Command gateway is the Kafka3O Gateway entrypoint (TECH-SPEC §5.1).
//
// Task 1.1 placeholder: it only reports its version so the module builds, lints,
// and tests from the first task. Task 1.10 replaces this with the `serve`, `probe`,
// and `keygen` subcommands wired through internal/app.
package main

import (
	"fmt"
	"os"
)

// version is injected at build time: -ldflags "-X main.version=<tag>" (see Makefile).
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println(version)
		return
	}
	fmt.Fprintln(os.Stderr, "kafka3o-gateway: not wired yet — see TASKS.md Task 1.10")
	os.Exit(2)
}
