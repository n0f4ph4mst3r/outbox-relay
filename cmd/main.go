package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/n0f4ph4mst3r/outbox-relay/internal/config"
	"github.com/n0f4ph4mst3r/outbox-relay/internal/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	cfg := config.MustLoad()
	fmt.Printf("Config loaded: %+v\n", cfg)

	log := setupLogger(cfg.Env)

	log.Info("Starting application...", slog.String("env", cfg.Env))
	log.Debug("Debugging is enabled")

	signalCtx, signalCancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer signalCancel()

	dbPool, err := pgxpool.New(signalCtx, cfg.DatabaseURL)
	if err != nil {
		panic(fmt.Sprintf("Unable to connect to database: %v", err))
	}

	var retriesCount int
	for retriesCount = 0; retriesCount < 10; retriesCount++ {
		err = dbPool.Ping(signalCtx)
		if nil == err {
			break
		}

		select {
		case <-signalCtx.Done():
			panic(signalCtx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}

	if retriesCount == 10 {
		panic("Unable to connect to database after 10 attempts: " + err.Error())
	}

	brokerOpts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Broker.BrokerURL),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RequestRetries(5),
	}

	log.Info("Initializing kafka client...")
	kgoClient, err := kgo.NewClient(brokerOpts...)
	if err != nil {
		panic(fmt.Sprintf("Failed to create kafka client: %v", err))
	}
	log.Info("Kafka client initialized successfully")

	outboxWorker := outbox.NewWorker(log, dbPool, kgoClient)

	errg, errgroupCtx := errgroup.WithContext(signalCtx)
	errg.Go(func() error {
		log.Info("Starting Outbox Worker...")
		outboxWorker.Start(errgroupCtx, cfg.Worker.EventTTL, cfg.Worker.BatchLimit)
		return nil
	})

	if err := errg.Wait(); err != nil {
		log.Error("Outbox worker exited with error", slog.Any("err", err))
	} else {
		log.Info("Worker finished successfully")
	}

	log.Info("Application stopped")
}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case envDev:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case envProd:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	return log
}
