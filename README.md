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

## Caveats

- While rebalancing any order between messages is lost, except for the time
ordering.
