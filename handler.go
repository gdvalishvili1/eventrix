package eventrix

import "context"

type EventHandlerResult string

const (
	Success       EventHandlerResult = "success"
	RetryableErr  EventHandlerResult = "retryable"
	PermanentErr  EventHandlerResult = "permanent"
	DeadLetterErr EventHandlerResult = "dead-letter"
)

// EventHandlerFunc defines a function that processes a Kafka event message.
type EventHandlerFunc func(ctx context.Context, key string, value []byte) (EventHandlerResult, error)

// RegisterHandler registers a handler for a specific event type
func (c *Consumer) RegisterHandler(eventType string, handler EventHandlerFunc) {
	c.handlerLock.Lock()
	defer c.handlerLock.Unlock()
	c.handlers[eventType] = handler
	c.log("info", "Registered handler", map[string]any{
		"event_type": eventType,
	})
}
