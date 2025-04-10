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
