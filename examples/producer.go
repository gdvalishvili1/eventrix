package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaProducer struct {
	writer *kafka.Writer
	topic  string
}

func NewKafkaProducer(brokers []string) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		Compression:  kafka.Snappy,
	}

	return &KafkaProducer{
		writer: writer,
	}
}

func (p *KafkaProducer) Produce(topic string, key string, message any, eventType string) error {
	value, err := json.Marshal(message)
	if err != nil {
		return err
	}

	headers := []kafka.Header{
		{
			Key:   "event-type",
			Value: []byte(eventType),
		},
	}

	msg := kafka.Message{
		Key:     []byte(key),
		Value:   value,
		Time:    time.Now(),
		Headers: headers,
	}

	if topic != "" {
		msg.Topic = topic
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return p.writer.WriteMessages(ctx, msg)
}
