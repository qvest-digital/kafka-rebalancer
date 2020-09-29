# Kafka Rebalancer

This is used, when you change the number of partitions, or you want to migrate
to a new balancing hash algorithm.


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
err := rebalance.Topic(readerConfig, "vehiclesV2", &kafka.Hash{})
if err != nil {
	return fmt.Errorf("rebalancing vehilces to vehiclesV2: %w", err)
}
```

## Progress of rebalancing in rebalancer

1. Read till high watermark
2. Sort theses messages by time
3. Write these messages with new balancer in new topic
4. Replicate new messages in existing topic to new topic (without checking the
   time, to ensure right ordering current producers should be switched to new
   balance algorithm before rebalancing with this tool)

## Steps to rebalancer your messages

1. Switch current services to new balancing algorithm
2. Execute this rebalance script
3. Switch current services to new topics

## Caveats

- While rebalancing any order between messages is lost, except for the time
ordering.


## k8s

```
./bin/build.sh --docker-repository [ECR-URL] --aws
./bin/run-k8s.sh --image [ECR-URL]
```
