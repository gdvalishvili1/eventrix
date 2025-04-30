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
