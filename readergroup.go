package rebalance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

// InitReaderGroup initializes a group of readers for the provided partitions
// when a partitions slice of the length 0 is passed the group is
// initialized with readers for all partitions of the provided topic
// this is not a ConsumerGroup in the sense of kafka. If you want to use ConsumerGroups have a look at NewReader
func initReaderGroup(ctx context.Context, config kafka.ReaderConfig, partitions ...int) (*readerGroup, error) {
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
	}

	for i := range partitions {
		config.Partition = partitions[i]
		group.readers[i] = kafka.NewReader(config)
	}

	go group.startFetching(ctx)

	return &group, nil
}

type readerGroup struct {
	readers  []*kafka.Reader
	messages chan fetchMessageResponse
}

type fetchMessageResponse struct {
	msg kafka.Message
	err error
}

// Close closes the connections
func (g *readerGroup) Close() error {
	defer close(g.messages)

	var errg errgroup.Group

	for i := range g.readers {
		errg.Go(g.readers[i].Close)
	}

	return errg.Wait()
}

func (g *readerGroup) startFetching(ctx context.Context) {
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

func (g *readerGroup) Lag() int64 {
	var lag int64
	// TODO what about the readers, which haven't fetched a message, yet (they might return 0)
	for i := range g.readers {
		lag += g.readers[i].Lag()
	}
	return lag
}

func (g *readerGroup) ReadLag(ctx context.Context) (int64, error) {
	var errg errgroup.Group

	// we are using the slice, because we are not writing concurrently
	// on multiple indices
	lags := make([]int64, len(g.readers))
	go func() {
		for i := range g.readers {
			// copy to have the value in the closure (i is incremented by the loop)
			n := i
			errg.Go(func() error {
				var err error
				lags[n], err = g.readers[n].ReadLag(ctx)
				if err != nil {
					return err
				}
				return nil
			})
		}
	}()

	err := errg.Wait()
	if err != nil {
		return 0, err
	}

	var totalLag int64
	for _, lag := range lags {
		totalLag += lag
	}

	return totalLag, nil
}
