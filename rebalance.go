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

	logger := log.With().Str("targetTopic", targetTopic).Logger()

	return rebalanceTopic(ctx, logger, group, writer)
}

type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	ReadLag(ctx context.Context) (int64, error)
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

func rebalanceTopic(ctx context.Context, logger zerolog.Logger, reader messageReader, writer messageWriter) error {
	lag, err := reader.ReadLag(ctx)
	if err != nil {
		return fmt.Errorf("reading initial lag: %w", err)
	}

	// TODO what about message keys not transformable to strings
	var msgs map[string][]kafka.Message
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
		if msgs[string(msg.Key)] == nil {
			msgs[string(msg.Key)] = []kafka.Message{}
		}
		msgs[string(msg.Key)] = append(msgs[string(msg.Key)], msg)
		n++
	}

	// TODO offset merken und für consumer groups schreiben (OFFSET AUS DEM NEUEN TOPIC)

	logger.Info().
		Uint64("msgAmount", n).
		Msg("Retrieved till high water mark")

	// TODO sort messages by timestamp
	// TODO write messages
	// TODO read new messages and write them until this rebalancer is cancelled to restart the components listening to the new topics / consumer group offset aktualisieren

	return nil
}

func messagesTillHighWatermark() (kafka.Message, error) {

}
