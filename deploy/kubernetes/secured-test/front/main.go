// Command kafka3o-secured-front serves the secured UI on the pod's forwarded loopback port and relays the
// Console API to the Console's HTTPS loopback listener. It holds no credentials and is not user authentication.
package main

import (
	"crypto/x509"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 4 {
		log.Fatal("usage: kafka3o-secured-front <ui-directory> <console-origin> <console-ca-file>")
	}
	data, err := os.ReadFile(os.Args[3])
	if err != nil {
		log.Fatalf("read Console CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		log.Fatal("the Console CA file has no certificates")
	}
	relay, err := newConsoleRelay(os.Args[2], roots)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{
		Addr: "127.0.0.1:5173", Handler: newFrontHandler(os.Args[1], relay),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	log.Fatal(server.ListenAndServe())
}
