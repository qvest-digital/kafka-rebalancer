package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	rebalance "github.com/fvosberg/kafka-rebalancer"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, shutdown := context.WithCancel(context.Background())
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)

	runErr := make(chan error, 1)

	go func() {
		runErr <- run(ctx, os.Args[1:], logger)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-runErr:
		if err != nil {
			log.Err(err).Msg("Critical error occurred")
			os.Exit(1)
		}
	case sig := <-signals:
		logger.Info().
			Str("sig", sig.String()).
			Msg("Got OS signal for shutdown")
		shutdown()
	}
}

func run(ctx context.Context, args []string, log zerolog.Logger) error {
	readerConfig := kafka.ReaderConfig{
		Brokers: []string{""},
	}
	var targetTopic string
	var useTLS bool

	flags := flag.NewFlagSet("kafka-rebalancer", flag.ContinueOnError)
	flags.StringVar(&readerConfig.Topic, "base-topic", "", "the topic, where messages are read from")
	flags.StringVar(&readerConfig.Brokers[0], "broker-url", "", "the url to the kafka broker")
	flags.BoolVar(&useTLS, "use-tls", false, "use TLS for the kafka connection")
	flags.StringVar(&targetTopic, "target-topic", "", "the topic, where messages are migrated to")

	err := flags.Parse(args)
	if err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}

	if readerConfig.Topic == "" ||
		readerConfig.Brokers[0] == "" ||
		targetTopic == "" {
		flags.PrintDefaults()
		return errors.New("missing config")
	}

	if useTLS {
		readerConfig.Dialer = kafka.DefaultDialer
		readerConfig.Dialer.TLS = &tls.Config{}
	}

	return rebalance.Topic(ctx, log, readerConfig, targetTopic, &kafka.Hash{})
}
