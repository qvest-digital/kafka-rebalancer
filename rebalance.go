package rebalance

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	FetchMessage(ctx context.Context) (kafka.Message, error)
	ReadLag(ctx context.Context) (int64, error)
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type rebalancer struct {
	log      zerolog.Logger
	messages messageStore
}

type messageStore interface {
	AddMessages(msg ...kafka.Message)
	Messages() []kafka.Message
}

func (r *rebalancer) rebalanceTopic(ctx context.Context, reader messageReader, writer messageWriter) error {
	lag, err := reader.ReadLag(ctx)
	if err != nil {
		return fmt.Errorf("reading initial lag: %w", err)
	}

	var n uint64

	for lag > 0 {
		fetchCtx, cancelFetch := context.WithTimeout(ctx, 1*time.Second)
		msg, err := reader.FetchMessage(fetchCtx)
		cancelFetch()
		if errors.Is(err, context.DeadlineExceeded) {
			lag, err = reader.ReadLag(ctx)
			if err != nil {
				return fmt.Errorf("reading lag: %w", err)
			}
			continue
		} else if err != nil {
			return fmt.Errorf("fetching message: %w", err)
		}
		r.messages.AddMessages(msg)
		n++
	}

	// TODO offset merken und für consumer groups schreiben (OFFSET AUS DEM NEUEN TOPIC)

	r.log.Info().
		Uint64("msgAmount", n).
		Msg("Retrieved till high water mark")

	// TODO write messages
	// TODO read new messages and write them until this rebalancer is cancelled to restart the components listening to the new topics / consumer group offset aktualisieren

	return nil
}

func (r *rebalancer) messagesTillHighWatermark() (kafka.Message, error) {
	return kafka.Message{}, nil
}
