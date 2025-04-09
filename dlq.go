package eventrix

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

// sendToDLQ sends a message to the dead letter queue
func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, processingErr error) {
	if !c.dlqEnabled || c.writer == nil {
		return
	}

	headers := append([]kafka.Header(nil), msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "error-message", Value: []byte(processingErr.Error())},
		kafka.Header{Key: "original-topic", Value: []byte(msg.Topic)},
		kafka.Header{Key: "original-partition", Value: []byte(fmt.Sprintf("%d", msg.Partition))},
		kafka.Header{Key: "original-offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
		kafka.Header{Key: "dlq-timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
	)

	err := c.writer.WriteMessages(ctx, kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})

	if err != nil {
		c.log("error", "Failed to send message to DLQ", map[string]any{
			"error": err.Error(),
			"key":   string(msg.Key),
		})
	} else {
		c.log("info", "Sent message to DLQ", map[string]any{
			"key": string(msg.Key),
		})
	}
}
