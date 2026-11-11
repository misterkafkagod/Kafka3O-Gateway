package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
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

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
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

	secured, securedMode, err := config.LoadSecuredMode(cfg.Security)
	if err != nil {
		logger.Error("invalid gateway secured-mode configuration", "error", err)
		os.Exit(1)
	}
	token := os.Getenv("KAFKA3O_GATEWAY_TOKEN")
	var certificate tls.Certificate
	if securedMode {
		if token != "" {
			logger.Error("KAFKA3O_GATEWAY_TOKEN must not be set in secured mode; shared tokens cannot bypass Console policy")
			os.Exit(1)
		}
		certificate, err = config.LoadGatewayCertificate(cfg.HTTP.TLS, tls.LoadX509KeyPair)
	} else {
		certificate, err = config.LoadGatewaySecurity(cfg.HTTP.TLS, token, tls.LoadX509KeyPair)
	}
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
	getMetrics := metrics.NewGetMetrics(metrics.Sources{LogDirs: kafka.NewReadLogDirs(admin), Health: kafka.NewReadPartitionHealth(admin),
		Offsets: kafka.NewReadEndOffsets(admin), Quorum: kafka.NewReadQuorum(kafkaClient), Brokers: kafka.NewReadBrokerLeaders(admin)}, cfg.Kafka.Timeout, time.Now)
	listTopics := topics.NewListTopics(kafka.NewListTopicMetadata(kafkaClient), kafka.NewCountTopicMessages(admin), cfg.Kafka.Timeout)
	getTopicDetail := topics.NewGetTopicDetail(kafka.NewDescribeTopic(admin), cfg.Kafka.Timeout)
	listLatestTopicMessages := topics.NewListLatestTopicMessages(kafka.NewListLatestTopicMessages(admin, kafkaClient), cfg.Kafka.Timeout)
	readTopicMessages := topics.NewReadTopicMessages(kafka.NewReadTopicMessages(admin, kafkaClient), cfg.Kafka.Timeout)
	searchTopicMessages := topics.NewSearchTopicMessages(kafka.NewFetchRecentTopicMessages(admin, kafkaClient), cfg.Kafka.Timeout)
	produceTopicMessage := topics.NewProduceTopicMessage(kafka.NewSendTopicMessage(producer), cfg.Kafka.Timeout)
	createTopic := topics.NewCreateTopic(kafka.NewCreateKafkaTopic(writeAdmin), cfg.Kafka.Timeout)
	deleteTopic := topics.NewDeleteTopic(kafka.NewDeleteKafkaTopic(writeAdmin), cfg.Kafka.Timeout)
	listConsumerGroups := groups.NewListConsumerGroups(kafka.NewFetchConsumerGroups(admin), cfg.Kafka.Timeout)
	groupInactive := kafka.NewEnsureGroupInactive(admin)
	previewOffsetReset := groups.NewPreviewOffsetReset(kafka.NewComputeOffsetResetPreview(groupInactive, admin), cfg.Kafka.Timeout)
	applyOffsetReset := groups.NewApplyOffsetReset(kafka.NewCommitOffsetReset(groupInactive, admin, writeAdmin), cfg.Kafka.Timeout)
	connectClient := &http.Client{Transport: http.DefaultTransport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	listConnectors := connect.NewListConnectors(connect.NewFetchConnectConnectors(cfg.Connect.URL, connectClient), cfg.Kafka.Timeout)
	manageConnector := connect.NewManageConnector(connect.NewSendConnectAction(cfg.Connect.URL, connectClient), cfg.Kafka.Timeout)
	backupSettings := backup.Settings{S3Endpoint: cfg.Backup.S3Endpoint, Bucket: cfg.Backup.Bucket, Region: cfg.Backup.Region, Prefix: cfg.Backup.Prefix}
	startBackup := backup.NewStartBackup(backupSettings, connect.NewCreateConnector(cfg.Connect.URL, connectClient), cfg.Kafka.Timeout)
	backupObjects := s3store.NewListObjects(cfg.Backup.S3Endpoint, cfg.Backup.Bucket, cfg.Backup.Region,
		s3store.Credentials{AccessKey: cfg.Backup.ReadAccessKey, SecretKey: cfg.Backup.ReadSecretKey}, connectClient, time.Now)
	backupReadCredentials := s3store.Credentials{AccessKey: cfg.Backup.ReadAccessKey, SecretKey: cfg.Backup.ReadSecretKey}
	checkRestore := backup.NewCheckRestore(backupSettings, connect.NewFetchConnectorConfig(cfg.Connect.URL, connectClient), backupObjects,
		backup.NewScanObjectRecords(s3store.NewGetObject(cfg.Backup.S3Endpoint, cfg.Backup.Bucket, cfg.Backup.Region, backupReadCredentials, connectClient, time.Now)),
		kafka.NewReadConnectOffsets(admin, kafkaClient, cfg.Connect.OffsetsTopic), manageConnector)
	listBackups := backup.NewListBackups(backupSettings, listConnectors, backupObjects, checkRestore, connect.NewFetchConnectorConfig(cfg.Connect.URL, connectClient))
	manageBackup := backup.NewManageBackup(backupSettings, manageConnector)
	restoreID := func() string {
		suffix := make([]byte, 4)
		_, _ = rand.Read(suffix)
		return "r-" + time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(suffix)
	}
	startRestore := backup.NewStartRestore(backupSettings, listConnectors, connect.NewFetchConnectorConfig(cfg.Connect.URL, connectClient), backupObjects, manageConnector,
		connect.NewCreateConnector(cfg.Connect.URL, connectClient), restoreID, cfg.Kafka.Timeout)
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
		createTopic: createTopic, deleteTopic: deleteTopic, listConsumerGroups: listConsumerGroups, previewOffsetReset: previewOffsetReset, applyOffsetReset: applyOffsetReset, listConnectors: listConnectors, manageConnector: manageConnector, startBackup: startBackup, listBackups: listBackups, manageBackup: manageBackup, startRestore: startRestore, getMetrics: getMetrics,
		getConfigurations: getConfigurations, alterConfiguration: alterConfiguration,
		increasePartitions: increasePartitions, listReassignments: listReassignments, reassignPartition: reassignPartition,
	})

	var server *http.Server
	if securedMode {
		policy, err := gatewayauth.NewConsolePolicyClient(secured.PolicyURL, secured.Identity, secured.ServiceCA)
		if err != nil {
			logger.Error("invalid Console policy client", "error", err)
			os.Exit(1)
		}
		handler, err := gatewayauth.WithDelegatedAuthorization(mux, policy, secured.ClusterID, logger)
		if err != nil {
			logger.Error("invalid secured-mode handler", "error", err)
			os.Exit(1)
		}
		server = newSecuredGatewayHTTPServer(cfg.HTTP.Listen, handler, certificate, secured.ServiceCA)
		logger.Info("gateway secured mode: only the Console with a current delegation may call Kafka routes", "cluster", secured.ClusterID)
	} else {
		server = newGatewayHTTPServer(cfg.HTTP.Listen, mux, cfg.HTTP.CORS.AllowedOrigins, token, certificate)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("gateway listening with TLS", "addr", cfg.HTTP.Listen)
		if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			stop()
		}
	}()
	var metricsServer *http.Server
	if cfg.Metrics.Listen != "" {
		metricsServer = newMetricsHTTPServer(cfg.Metrics.Listen, httpapi.NewPrometheusHandler(getMetrics), cfg.Metrics.Token, certificate)
		go func() {
			logger.Info("metrics listening with TLS", "addr", cfg.Metrics.Listen)
			if err := metricsServer.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server failed", "error", err)
				stop()
			}
		}()
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "error", err)
	}
	if metricsServer != nil {
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("metrics shutdown failed", "error", err)
		}
	}
	logger.Info("gateway stopped")
}
