package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
)

// main is the composition root: it loads config, wires dependencies and serves HTTP.
func main() {
	configPath := flag.String("config", "", "path to the YAML config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	kafkaClient, err := kafka.NewClient(cfg.Kafka)
	if err != nil {
		logger.Error("invalid kafka client configuration", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()
	admin := kadm.NewClient(kafkaClient)

	getCluster := cluster.NewGetCluster(kafka.NewDescribeCluster(admin), cfg.Kafka.Timeout)

	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Kafka3O Gateway", "0.1.0"))
	httpapi.RegisterLive(api)
	httpapi.RegisterGetCluster(api, getCluster)

	server := &http.Server{
		Addr:              cfg.HTTP.Listen,
		Handler:           httpapi.WithCORS(mux, cfg.HTTP.CORS.AllowedOrigins),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("gateway listening", "addr", cfg.HTTP.Listen)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "error", err)
	}
	logger.Info("gateway stopped")
}
