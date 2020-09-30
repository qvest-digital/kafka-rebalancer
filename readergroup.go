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
			dialer = kafka.DefaultDialer
		}
		pp, err := dialer.LookupPartitions(
			ctx,
			"tcp",
			config.Brokers[0],
			config.Topic,
		)
		if err != nil {
			return nil, fmt.Errorf("looking up partitions: %w", err)
		}
		partitions = make([]int, len(pp))
		for i := range pp {
			partitions[i] = pp[i].ID
		}
	}

	group := readerGroup{
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

	// the reader doesn't support returning the hwm, but we can calc it
	// We don't want to use the lag, because it is just the difference between
	// current offset and hwm. Due to compaction some offsets might be missing
	hwms := make(map[int]int64, len(g.readers))

	var errg errgroup.Group
	for i := range g.readers {
		n := i
		errg.Go(func() error {
			lag, err := g.readers[n].ReadLag(ctx)
			hwms[n] = lag + g.readers[n].Offset()
			return err
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

			for g.readers[i].Offset() <= hwms[i] {
				msg, err := g.readers[i].FetchMessage(ctx)
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				g.tillHighWatermark <- fetchMessageResponse{
					msg: msg,
					err: err,
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(g.tillHighWatermark)
	}()

	return nil
}

func (g *readerGroup) StartFetching(ctx context.Context) {
	g.messages = make(chan fetchMessageResponse)

	var wg sync.WaitGroup
	wg.Add(len(g.readers))

	for i := range g.readers {
		go func(reader *kafka.Reader) {
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
