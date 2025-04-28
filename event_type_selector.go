package eventrix

import "github.com/segmentio/kafka-go"

type EventTypeSelector interface {
	Select(message kafka.Message) string
}

type EventTypeHeaderSelector struct {
	headerKey string
}

func NewEventTypeHeaderSelector(headerKey string) *EventTypeHeaderSelector {
	return &EventTypeHeaderSelector{headerKey: headerKey}
}

func (s *EventTypeHeaderSelector) Select(msg kafka.Message) string {
	var eventType string
	for _, h := range msg.Headers {
		if h.Key == s.headerKey {
			eventType = string(h.Value)
			break
		}
	}

	return eventType
}

var _ EventTypeSelector = (*EventTypeHeaderSelector)(nil)
