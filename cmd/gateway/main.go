package main

import (
	"context"
	"crypto/tls"
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
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
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

	token := os.Getenv("KAFKA3O_GATEWAY_TOKEN")
	certificate, err := config.LoadGatewaySecurity(cfg.HTTP.TLS, token, tls.LoadX509KeyPair)
	if err != nil {
		logger.Error("invalid gateway security configuration", "error", err)
		os.Exit(1)
	}

	kafkaClient, err := kafka.NewClient(cfg.Kafka)
	if err != nil {
		logger.Error("invalid kafka client configuration", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()
	admin := kadm.NewClient(kafkaClient)
	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		logger.Error("invalid kafka producer configuration", "error", err)
		os.Exit(1)
	}
	defer producer.Close()
	writeAdmin := kadm.NewClient(producer)

	getCluster := cluster.NewGetCluster(kafka.NewDescribeCluster(kafkaClient), cfg.Kafka.Timeout)
	listTopics := topics.NewListTopics(kafka.NewListTopicMetadata(kafkaClient), kafka.NewCountTopicMessages(admin), cfg.Kafka.Timeout)
	getTopicDetail := topics.NewGetTopicDetail(kafka.NewDescribeTopic(admin), cfg.Kafka.Timeout)
	listLatestTopicMessages := topics.NewListLatestTopicMessages(kafka.NewListLatestTopicMessages(admin, kafkaClient), cfg.Kafka.Timeout)
	readTopicMessages := topics.NewReadTopicMessages(kafka.NewReadTopicMessages(admin, kafkaClient), cfg.Kafka.Timeout)
	searchTopicMessages := topics.NewSearchTopicMessages(kafka.NewFetchRecentTopicMessages(admin, kafkaClient), cfg.Kafka.Timeout)
	produceTopicMessage := topics.NewProduceTopicMessage(kafka.NewSendTopicMessage(producer), cfg.Kafka.Timeout)
	createTopic := topics.NewCreateTopic(kafka.NewCreateKafkaTopic(writeAdmin), cfg.Kafka.Timeout)
	deleteTopic := topics.NewDeleteTopic(kafka.NewDeleteKafkaTopic(writeAdmin), cfg.Kafka.Timeout)
	listConsumerGroups := groups.NewListConsumerGroups(kafka.NewFetchConsumerGroups(admin), cfg.Kafka.Timeout)
	describeConfigurations := kafka.NewDescribeConfigurations(kafkaClient)
	getConfigurations := settings.NewGetConfigurations(describeConfigurations, cfg.Kafka.Timeout)
	alterConfiguration := settings.NewAlterConfiguration(describeConfigurations, kafka.NewAlterConfiguration(writeAdmin), cfg.Kafka.Timeout)
	describePlacement := kafka.NewDescribePlacement(kafkaClient)
	listKafkaReassignments := kafka.NewListKafkaReassignments(admin)
	increasePartitions := partitions.NewIncreasePartitionCount(describePlacement, kafka.NewUpdateKafkaPartitions(writeAdmin), cfg.Kafka.Timeout)
	listReassignments := partitions.NewListReassignments(describePlacement, listKafkaReassignments, cfg.Kafka.Timeout)
	reassignPartition := partitions.NewReassignPartition(describePlacement, listKafkaReassignments, kafka.NewAlterKafkaReassignment(writeAdmin), cfg.Kafka.Timeout)

	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Kafka3O Gateway", "0.1.0"))
	registerGatewayRoutes(api, gatewayActions{
		getCluster: getCluster, listTopics: listTopics, getTopicDetail: getTopicDetail,
		listLatestTopicMessages: listLatestTopicMessages, readTopicMessages: readTopicMessages,
		searchTopicMessages: searchTopicMessages, produceTopicMessage: produceTopicMessage,
		createTopic: createTopic, deleteTopic: deleteTopic, listConsumerGroups: listConsumerGroups,
		getConfigurations: getConfigurations, alterConfiguration: alterConfiguration,
		increasePartitions: increasePartitions, listReassignments: listReassignments, reassignPartition: reassignPartition,
	})

	server := newGatewayHTTPServer(cfg.HTTP.Listen, mux, cfg.HTTP.CORS.AllowedOrigins, token, certificate)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("gateway listening with TLS", "addr", cfg.HTTP.Listen)
		if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
