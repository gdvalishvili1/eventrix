# eventrix
Kafka consumer for golang with partition-ordered concurrent processing and event handling

Built on top of [kafka-go](https://github.com/segmentio/kafka-go)

Inspired by [Parallel consumer](https://github.com/confluentinc/parallel-consumer)

# Problem
The unit of parallelism in Kafka’s consumers is the partition but sometimes you want additional parallelism using threads per consumer instance rather than new instances of a Consumer

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
	"github.com/gdvalishvili1/eventrix"
	"github.com/google/uuid"
	"log"
	"time"
)

func main() {
	consumer := eventrix.NewConsumer(
		[]string{"localhost:29092"},
		"test-app",
		[]string{"accounts.topic"},
		&eventrix.ProcessingOptions{
			HandlerTimeout:        10 * time.Second,
			MaxConcurrentMessages: 50,
			RetryBackoff:          []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second},
		},
	)

	consumer.RegisterHandler("UserRegistered", NewBranchCreatedHandler())

	if err := consumer.Start(context.Background()); err != nil {
		println("Error starting consumer:", err)
	}
}

type UserRegisteredHandler struct {
}

func NewBranchCreatedHandler() *UserRegisteredHandler {
	return &UserRegisteredHandler{}
}

func (h *UserRegisteredHandler) Handle(ctx context.Context, key string, value []byte) (eventrix.EventHandlerResult, error) {
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
