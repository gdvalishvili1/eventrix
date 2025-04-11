package eventrix

import "github.com/segmentio/kafka-go"

type EventTypeSelector interface {
	Select(message kafka.Message) string
}

type EventTypeHeaderSelector struct {
}

func NewEventTypeHeaderSelector() *EventTypeHeaderSelector { return &EventTypeHeaderSelector{} }

func (h *EventTypeHeaderSelector) Select(msg kafka.Message) string {
	var eventType string
	for _, h := range msg.Headers {
		if h.Key == "event-type" {
			eventType = string(h.Value)
			break
		}
	}

	return eventType
}

var _ EventTypeSelector = (*EventTypeHeaderSelector)(nil)
