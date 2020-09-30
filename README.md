# Kafka Rebalancer

This is used, when you change the number of partitions, or you want to migrate
to a new balancing hash algorithm.

## Concepts

### Progress of rebalancing in rebalancer

1. Read till high watermark
2. Sort theses messages by time
3. Write these messages with new balancer in new topic
4. Replicate new messages in existing topic to new topic (without checking the
   time, to ensure right ordering current producers should be switched to new
   balance algorithm before rebalancing with this tool)

### Steps to rebalancer your messages

1. Switch current services to new balancing algorithm
2. Execute this rebalance script
3. Switch current services to new topics

### Caveats

- While rebalancing any order between messages is lost, except for the time
ordering.

## Usage

### Usage as a library

``` golang
readerConfig := &kafka.ReaderConfig{
	Brokers: kafkaURLs,
	Dialer: &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true, // IPv4 and IPv6
		TLS:       tlsConfig,
	},
	Topic:            "vehicles",
}
err := rebalance.Topic(
	context.Background(),
	log.Logger,
	readerConfig,
	"vehiclesV2",
	&kafka.Hash{},
)
```

### Usage as a CLI

```
go run ./cmds/rebalance/ -broker-url kafka:9092 -base-topic vehicles -target-topic vehicleV2 (-use-tls)
```

### Usage in kubernetes

You can build an image, push it to your registry and start a pod running the
rebalancer. You have to provide the command line flags for the rebalancer.

```
./bin/build.sh --image [docker-image-url] (--aws)
./bin/run-k8s.sh --image [docker-image-url] (--suffix [pod-name-suffix]) (args)
```
