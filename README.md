# eventrix
Kafka consumer for golang with partition-ordered concurrent processing and event handling

Built on top of [kafka-go](https://github.com/segmentio/kafka-go)

Inspired by [Parallel consumer](https://github.com/confluentinc/parallel-consumer)

# Problem
In Kafka, the unit of parallelism is the partition — each partition is consumed by only one consumer within a group. If a topic has more partitions than consumer instances (e.g., 10 partitions and 2 consumers), each consumer handles multiple partitions.

By default, each consumer processes messages sequentially per partition, which can become a bottleneck. When you need higher throughput but still want to preserve message order within each partition, you need a multi-threaded consumer that can process messages concurrently per partition

kafka-go default behavior doesn't support multi-threaded message processing per consumer with partition-level ordering guarantees out of the box.

# Features
* Process messages concurrently while maintaining Kafka partition ordering guarantees
* Consumers with multiple topics, event types and separate handlers
* Batched offset commits with configurable intervals after successful processing to avoid message loss
* Configure concurrent message processing limits
* Automatic retries with configurable backoff strategy
* Dead letter queue for failed messages
* Extensible metrics and logging callbacks
* Health check APIs for monitoring

# Installation
```
go get github.com/gdvalishvili1/eventrix
```

# Quick Start

```
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gdvalishvili1/eventrix"
	"github.com/google/uuid"
	"log"
	"time"
)

func main() {
	ctx := context.Background()
	producer := NewKafkaProducer([]string{"localhost:29092"})

	for i := 0; i < 10; i++ {
		err := producer.Produce("accounts.topic", uuid.NewString(), UserRegistered{Name: "test"}, "UserRegistered")
		if err != nil {
			panic(err)
		}
		fmt.Printf("Producing message %v\n", i)
	}

	println("waiting for producer...")

	go startConsuming()

	<-ctx.Done()
}

func startConsuming() {
	consumer, err := eventrix.NewConsumer(
		[]string{"localhost:29092"},
		"test-app",
		[]string{"accounts.topic"},
		&eventrix.ProcessingOptions{
			HandlerTimeout:        10 * time.Second,
			MaxConcurrentMessages: 50,
			RetryBackoff:          []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second},
		},
		nil,
	)

	if err != nil {
		panic(err)
	}

	handler := NewUserEventHandler()
	consumer.RegisterHandler("UserRegistered", handler.Register)

	if err := consumer.Start(context.Background()); err != nil {
		println("Error starting consumer:", err)
	}
}

type UserEventHandler struct {
}

func NewUserEventHandler() *UserEventHandler {
	return &UserEventHandler{}
}

func (h *UserEventHandler) Register(ctx context.Context, key string, value []byte) (eventrix.EventHandlerResult, error) {
	var event UserRegistered
	if err := json.Unmarshal(value, &event); err != nil {
		log.Printf("failed to unmarshal UserRegistered: %v", err)
		return "", err
	}

	log.Printf("handling UserRegistered event: %+v", event)

	return eventrix.Success, nil
}

type UserRegistered struct {
	Id   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

```

# Handler Results
Handlers must return one of these result types:

* Success: Message was processed successfully
* RetryableErr: Temporary error, retry with backoff
* PermanentErr: Permanent error, send to DLQ
* DeadLetterErr: Explicitly send to DLQ


# Contributing
Contributions are welcome! Please feel free to submit a Pull Request.
