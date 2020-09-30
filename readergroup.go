package rebalance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////
// This is a copy of a work in progress from a PR to kafka-go
////////////////////////////////////////////////////////////////////////////

// InitReaderGroup initializes a group of readers for the provided partitions
// when a partitions slice of the length 0 is passed the group is
// initialized with readers for all partitions of the provided topic
// this is not a ConsumerGroup in the sense of kafka. If you want to use ConsumerGroups have a look at NewReader
func initReaderGroup(ctx context.Context, log zerolog.Logger, config kafka.ReaderConfig, partitions ...int) (*readerGroup, error) {
	if config.GroupID != "" {
		return nil, errors.New("setting GroupID is not allowed")
	}
	if config.Partition != 0 {
		return nil, errors.New("setting partition is not allowed")
	}
	if len(partitions) == 0 {
		if len(config.Brokers) == 0 {
			return nil, errors.New("missing brokers")
		}
		dialer := config.Dialer
		if dialer == nil {
			config.Dialer = kafka.DefaultDialer
		}

		con, err := config.Dialer.DialContext(ctx, "tcp", config.Brokers[0])
		if err != nil {
			return nil, fmt.Errorf("dialing: %w", err)
		}

		defer con.Close()

		pp, err := con.ReadPartitions(config.Topic)
		if err != nil {
			return nil, fmt.Errorf("looking up partitions: %w", err)
		}
		partitions = make([]int, len(pp))
		for i := range pp {
			partitions[i] = pp[i].ID
		}
	}

	group := readerGroup{
		config:  config,
		readers: make([]*kafka.Reader, len(partitions)),
		log:     log,
	}

	for i := range partitions {
		config.Partition = partitions[i]
		group.readers[i] = kafka.NewReader(config)
	}

	err := group.startFetchingTillHighWatermark(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting fetchTillHighWatermark: %w", err)
	}

	return &group, nil
}

type readerGroup struct {
	log               zerolog.Logger
	config            kafka.ReaderConfig
	readers           []*kafka.Reader
	tillHighWatermark chan fetchMessageResponse
	messages          chan fetchMessageResponse
}

type fetchMessageResponse struct {
	msg kafka.Message
	err error
}

// Close closes the connections
func (g *readerGroup) Close() error {
	var errg errgroup.Group

	for i := range g.readers {
		errg.Go(g.readers[i].Close)
	}

	return errg.Wait()
}

func (g *readerGroup) startFetchingTillHighWatermark(ctx context.Context) error {
	g.tillHighWatermark = make(chan fetchMessageResponse)

	var wg sync.WaitGroup
	wg.Add(len(g.readers))

	hwms := make(map[int]int64, len(g.readers))

	var errg errgroup.Group
	for i := range g.readers {
		n := i
		errg.Go(func() error {
			con, err := g.config.Dialer.DialLeader(ctx, "tcp", g.config.Brokers[0], g.config.Topic, i)
			if err != nil {
				return fmt.Errorf("faield to dial: %w", err)
			}

			defer con.Close()

			offset, err := con.ReadLastOffset()
			if err != nil {
				return fmt.Errorf("faield to look up offset: %w", err)
			}

			g.log.Debug().Int64("offset", offset).Int("partition", i).Msg("fetched high water mark offset")
			hwms[n] = offset

			return nil
		})
	}
	err := errg.Wait()
	if err != nil {
		close(g.tillHighWatermark)
		return fmt.Errorf("reading high water mark: %w", err)
	}

	g.log.Info().
		Str("highwatermarks", fmt.Sprintf("%#v", hwms)).Msg("got hwms")

	for i := range g.readers {
		go func(i int) {
			defer wg.Done()

			for g.readers[i].Offset() < hwms[i] {
				logger := g.log.With().
					Int("partition", i).
					Int64("offset", g.readers[i].Offset()).
					Int64("hwm", hwms[i]).
					Logger()

				logger.Debug().Msg("fetching msg")

				msg, err := g.readers[i].FetchMessage(ctx)
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					logger.Err(err).Msg("ctx problem")
					return
				}

				logger.Debug().
					Bytes("key", msg.Key).
					Int("partition", msg.Partition).
					Int64("offset", msg.Offset).
					Time("time", msg.Time).
					Msg("fetched msg")

				g.tillHighWatermark <- fetchMessageResponse{
					msg: msg,
					err: err,
				}
			}

			g.log.Debug().
				Int("partition", i).
				Int64("offset", g.readers[i].Offset()).
				Int64("hwm", hwms[i]).
				Msg("finished reading till high water mark")
		}(i)
	}

	go func() {
		wg.Wait()
		close(g.tillHighWatermark)
		g.log.Debug().Msg("Closed high watermark chan")
	}()

	return nil
}

func (g *readerGroup) StartFetching(ctx context.Context) {
	g.messages = make(chan fetchMessageResponse)

	var wg sync.WaitGroup
	wg.Add(len(g.readers))

	for i := range g.readers {
		go func(reader *kafka.Reader) {
			g.log.Debug().Int("partition", reader.Config().Partition).Msg("Starting fetching")
			for {
				msg, err := reader.FetchMessage(ctx)
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					wg.Done()
					return
				}
				g.messages <- fetchMessageResponse{
					msg: msg,
					err: err,
				}
			}
		}(g.readers[i])
	}

	wg.Wait()
	close(g.messages)
}

func (g *readerGroup) FetchTillHighWatermark(ctx context.Context) (kafka.Message, error) {
	select {
	case res, ok := <-g.tillHighWatermark:
		if !ok {
			return kafka.Message{}, io.EOF
		}
		return res.msg, res.err
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (g *readerGroup) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case res, ok := <-g.messages:
		if !ok {
			return kafka.Message{}, io.EOF
		}
		return res.msg, res.err
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}
