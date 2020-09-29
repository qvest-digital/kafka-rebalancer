package rebalance

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

func Topic(ctx context.Context, readerConfig kafka.ReaderConfig, targetTopic string, balancer kafka.Balancer) error {
	group, err := initReaderGroup(ctx, readerConfig)
	if err != nil {
		return fmt.Errorf("init reader group: %w", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(readerConfig.Brokers[0]),
		Topic:        targetTopic,
		RequiredAcks: kafka.RequireAll,
		Balancer:     balancer,
	}

	rb := rebalancer{
		log:      log.With().Str("targetTopic", targetTopic).Logger(),
		messages: newMessageStore(),
	}

	return rb.rebalanceTopic(ctx, group, writer)
}

type messageReader interface {
	FetchTillHighWatermark(ctx context.Context) (kafka.Message, error)
	StartFetching(ctx context.Context)
	FetchMessage(ctx context.Context) (kafka.Message, error)
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type rebalancer struct {
	log      zerolog.Logger
	messages messageStore
}

//go:generate moq -out message_store_moq_test.go . messageStore
type messageStore interface {
	AddMessages(msg ...kafka.Message)
	Messages() []kafka.Message
}

func (r *rebalancer) rebalanceTopic(ctx context.Context, reader messageReader, writer messageWriter) error {

	err := r.readMessagesTillHighWatermark(ctx, reader)
	if err != nil {
		return fmt.Errorf("reading initial batch of messages till high watermark: %w", err)
	}
	err = writer.WriteMessages(ctx, r.messages.Messages()...)
	if err != nil {
		return fmt.Errorf("writing initial rebalancing batch of messages: %w", err)
	}
	// TODO offset merken und für consumer groups schreiben (OFFSET AUS DEM NEUEN TOPIC)
	// TODO read new messages and write them until this rebalancer is cancelled to restart the components listening to the new topics / consumer group offset aktualisieren
	// reader.StartFetching(ctx)
	// reader.FetchMessage(ctx)

	return nil
}

func (r *rebalancer) readMessagesTillHighWatermark(ctx context.Context, reader messageReader) error {
	var n uint64

	for {
		msg, err := reader.FetchTillHighWatermark(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("fetching message: %w", err)
		}
		r.messages.AddMessages(msg)
		n++
	}

	r.log.Info().
		Uint64("msgAmount", n).
		Msg("Retrieved till high water mark")

	return nil
}
