package eventrix

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"sync"
	"time"
)

type AuthType int

const (
	AuthScramSha256 AuthType = iota
	AuthScramSha512
	AuthPlain
)

type AuthOptions struct {
	Username string
	Password string
	AuthType AuthType
}

type topicPartition struct {
	topic     string
	partition int
}

const (
	OneKB = 1024
	OneMB = OneKB * 1024
)

type Consumer struct {
	reader              *kafka.Reader
	writer              *kafka.Writer // For dead letter queue
	handlers            map[string]EventHandler
	handlerLock         sync.RWMutex
	options             ProcessingOptions
	semaphore           chan struct{}                         // Limits concurrent message processing
	wg                  sync.WaitGroup                        // Tracks in-flight messages
	shutdownCh          chan struct{}                         // Signals shutdown
	commitCh            chan kafka.Message                    // Messages pending commit
	commitDoneCh        chan struct{}                         // Signals commit loop completion
	messageCounter      int64                                 // Total messages processed
	errorCounter        map[string]int64                      // Errors by type
	counterLock         sync.RWMutex                          // Guards counters
	healthy             bool                                  // Health status
	healthLock          sync.RWMutex                          // Guards health status
	dlqEnabled          bool                                  // Whether dead letter queue is enabled
	partitionProcessors map[topicPartition]chan kafka.Message // Per-partition processing channels
	partitionLock       sync.RWMutex                          // Guards partition processors map
}

func buildDialer(authOptions *AuthOptions) (*kafka.Dialer, error) {
	var dialer = &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	if authOptions != nil {
		if authOptions.Username == "" {
			return nil, errors.New("auth options must have a username")
		}
		if authOptions.Password == "" {
			return nil, errors.New("auth options must have a password")
		}

		var mechanism sasl.Mechanism
		var err error

		switch authOptions.AuthType {
		case AuthScramSha256:
			mechanism, err = scram.Mechanism(scram.SHA256, authOptions.Username, authOptions.Password)
		case AuthScramSha512:
			mechanism, err = scram.Mechanism(scram.SHA512, authOptions.Username, authOptions.Password)
		case AuthPlain:
			mechanism = plain.Mechanism{Username: authOptions.Username, Password: authOptions.Password}
		default:
			return nil, fmt.Errorf("unsupported auth type: %v", authOptions.AuthType)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}

		dialer.SASLMechanism = mechanism
	}

	return dialer, nil
}

// NewConsumer creates a new consumer
func NewConsumer(brokers []string, groupID string, topics []string, options *ProcessingOptions, authOptions *AuthOptions) (*Consumer, error) {
	opts := defaultOptions
	if options != nil {
		opts = *options
	}

	// handle defaults if values are invalid
	if opts.MaxConcurrentMessages <= 0 {
		opts.MaxConcurrentMessages = defaultOptions.MaxConcurrentMessages
	}
	if opts.HandlerTimeout <= 0 {
		opts.HandlerTimeout = defaultOptions.HandlerTimeout
	}
	if opts.CommitInterval <= 0 {
		opts.CommitInterval = defaultOptions.CommitInterval
	}
	if opts.EventTypeSelector == nil {
		opts.EventTypeSelector = defaultOptions.EventTypeSelector
	}
	if opts.EventTypeHeaderKey == "" {
		opts.EventTypeHeaderKey = defaultOptions.EventTypeHeaderKey
	}

	dialer, err := buildDialer(authOptions)

	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupID,
			GroupTopics:    topics,
			MinBytes:       OneKB * 1,  //TODO: move to processing options to set by client
			MaxBytes:       OneMB * 10, //TODO: move to processing options to set by client
			CommitInterval: opts.CommitInterval,
			MaxWait:        500 * time.Millisecond,
			Dialer:         dialer,
		}),
		handlers:            make(map[string]EventHandler),
		options:             opts,
		semaphore:           make(chan struct{}, opts.MaxConcurrentMessages),
		shutdownCh:          make(chan struct{}),
		commitCh:            make(chan kafka.Message, 1000),
		commitDoneCh:        make(chan struct{}),
		errorCounter:        make(map[string]int64),
		healthy:             true,
		dlqEnabled:          len(opts.DeadLetterTopic) > 0,
		partitionProcessors: make(map[topicPartition]chan kafka.Message),
	}

	if consumer.dlqEnabled {
		consumer.writer = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        opts.DeadLetterTopic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		}
	}

	return consumer, nil
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go c.commitLoop(ctx)

	// Main processing loop
	go func() {
		defer func() {
			// Close all partition processors
			c.partitionLock.Lock()
			for _, ch := range c.partitionProcessors {
				close(ch)
			}
			c.partitionLock.Unlock()

			// Signal commit loop to finish
			close(c.commitCh)
		}()

		for {
			// decide if it needs to shut down
			select {
			case <-c.shutdownCh:
				c.log("info", "Consumer shutting down", nil)
				return
			case <-ctx.Done():
				c.log("info", "Context canceled", nil)
				return
			default:
				// processing continues
			}

			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			m, err := c.reader.FetchMessage(readCtx)
			readCancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					continue
				}

				c.setHealthy(false)
				c.log("error", "Error fetching message", map[string]any{
					"error": err.Error(),
				})

				// Sleep briefly before retrying to avoid CPU spinning on persistent errors
				select {
				case <-time.After(time.Second):
				case <-c.shutdownCh:
					return
				case <-ctx.Done():
					return
				}
				continue
			}

			c.setHealthy(true)

			// To maintain ordering
			processor := c.getProcessorPerTopicChannel(m)

			select {
			case processor <- m:
			case <-c.shutdownCh:
				return
			case <-ctx.Done():
				// Context ended
				return
			}
		}
	}()

	<-ctx.Done()
	return nil
}

// Stop gracefully shuts down the consumer
func (c *Consumer) Stop() error {
	// Signal shutdown
	close(c.shutdownCh)

	// Wait for in-flight message processing to complete
	if err := c.waitForCompletion(30 * time.Second); err != nil {
		c.log("error", "Error waiting for completion", map[string]any{
			"error": err.Error(),
		})
	}

	// Wait for commit loop to complete
	<-c.commitDoneCh

	// Close Kafka connections
	if err := c.reader.Close(); err != nil {
		c.log("error", "Error closing Kafka reader", map[string]any{
			"error": err.Error(),
		})
	}

	if c.dlqEnabled && c.writer != nil {
		if err := c.writer.Close(); err != nil {
			c.log("error", "Error closing DLQ writer", map[string]any{
				"error": err.Error(),
			})
		}
	}

	return nil
}

// waitForCompletion waits for in process messages to complete processing
func (c *Consumer) waitForCompletion(timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("timed out waiting for messages to complete processing")
	}
}

// getProcessorPerTopicChannel returns a channel for processing messages from a specific partition
func (c *Consumer) getProcessorPerTopicChannel(message kafka.Message) chan<- kafka.Message {

	tp := topicPartition{
		topic:     message.Topic,
		partition: message.Partition,
	}

	c.partitionLock.RLock()
	ch, exists := c.partitionProcessors[tp]
	c.partitionLock.RUnlock()

	if exists {
		return ch
	}

	c.partitionLock.Lock()
	defer c.partitionLock.Unlock()

	if ch, exists = c.partitionProcessors[tp]; exists {
		return ch
	}

	// Create new processor channel
	ch = make(chan kafka.Message, 1000)
	c.partitionProcessors[tp] = ch

	// Start processor
	c.wg.Add(1)
	go c.processPartition(ch, tp)

	return ch
}

// processPartition processes messages from a single topic partition sequentially
func (c *Consumer) processPartition(ch <-chan kafka.Message, tp topicPartition) {
	defer c.wg.Done()
	c.log("info", "Started partition processor", map[string]any{
		"partition": tp,
	})

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				// Channel closed, exit
				c.log("info", "Partition processor shutting down", map[string]any{
					"partition": tp,
				})
				return
			}

			// Process the message sequentially
			// Acquire semaphore to limit overall concurrency
			select {
			case c.semaphore <- struct{}{}:
				c.processMessage(context.Background(), msg)
				<-c.semaphore
			case <-c.shutdownCh:
				return
			}

		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) {
	parentCtx := ctx

	eventType := c.options.EventTypeSelector.Select(msg)

	//TODO: make event type configurable by client code
	if eventType == "" {
		c.log("error", "Message has no event-type header", map[string]any{
			"key": string(msg.Key),
		})
		//TODO: Decide if move dlq on missing event type
		//if c.dlqEnabled {
		//	c.sendToDLQ(parentCtx, msg, ErrNoEventType)
		//}
		c.commitCh <- msg
		return
	}

	c.handlerLock.RLock()
	handler, ok := c.handlers[eventType]
	c.handlerLock.RUnlock()

	if !ok {
		c.log("error", "Unknown event type", map[string]any{
			"event_type": eventType,
			"key":        string(msg.Key),
		})
		//TODO: Decide if move dlq on unknown event type
		//if c.dlqEnabled {
		//	c.sendToDLQ(parentCtx, msg, ErrUnknownEventType)
		//}
		c.commitCh <- msg
		return
	}

	c.processMessageWithRetries(parentCtx, msg, handler, eventType)
}

func (c *Consumer) processMessageWithRetries(ctx context.Context, msg kafka.Message, handler EventHandler, eventType string) {
	key := string(msg.Key)
	var err error
	var result EventHandlerResult

	retryCount := 0
	for _, h := range msg.Headers {
		if h.Key == "retry-count" {
			fmt.Sscanf(string(h.Value), "%d", &retryCount)
			break
		}
	}

	handlerCtx, cancel := context.WithTimeout(ctx, c.options.HandlerTimeout)
	defer cancel()

	c.incrementCounter()

	// for metrics
	startTime := time.Now()

	// Execute event handler
	result, err = handler.Handle(handlerCtx, key, msg.Value)

	// Report metrics
	duration := time.Since(startTime)
	if c.options.MetricsCallback != nil {
		c.options.MetricsCallback("message_processing_time", float64(duration.Milliseconds()), map[string]string{
			"event_type": eventType,
			"result":     string(result),
		})
	}

	switch result {
	case Success:
		if err != nil {
			c.log("warn", "Handler returned success but with error", map[string]any{
				"event_type": eventType,
				"key":        key,
				"error":      err.Error(),
			})
		}
		c.commitCh <- msg

	case RetryableErr:
		if retryCount >= len(c.options.RetryBackoff) {
			c.log("error", "Max retries exceeded", map[string]any{
				"event_type": eventType,
				"key":        key,
				"error":      err.Error(),
				"retries":    retryCount,
			})
			if c.dlqEnabled {
				c.sendToDLQ(ctx, msg, err)
			}
			c.commitCh <- msg
		} else {
			c.log("warn", "Retrying message", map[string]any{
				"event_type": eventType,
				"key":        key,
				"error":      err.Error(),
				"retry":      retryCount + 1,
			})

			// Wait for backoff period
			backoff := c.options.RetryBackoff[retryCount]
			select {
			case <-time.After(backoff):
			case <-c.shutdownCh:
				return
			}

			// message will be redelivered
			c.incrementErrorCounter(eventType)
		}

	case PermanentErr, DeadLetterErr:
		c.log("error", "Permanent processing error", map[string]any{
			"event_type": eventType,
			"key":        key,
			"error":      err.Error(),
		})
		if c.dlqEnabled {
			c.sendToDLQ(ctx, msg, err)
		}
		c.commitCh <- msg
		c.incrementErrorCounter(eventType)

	default:
		c.log("error", "Unknown handler result", map[string]any{
			"event_type": eventType,
			"key":        key,
			"result":     string(result),
		})
		c.commitCh <- msg
		c.incrementErrorCounter(eventType)
	}

	if err != nil && c.options.ErrorCallback != nil {
		c.options.ErrorCallback(err, eventType, key, retryCount)
	}
}

func (c *Consumer) commitLoop(ctx context.Context) {
	defer close(c.commitDoneCh)

	// for periodic commits
	commitTicker := time.NewTicker(c.options.CommitInterval)
	defer commitTicker.Stop()

	var pendingMessages []kafka.Message

	for {
		select {
		case msg, ok := <-c.commitCh:
			if !ok {
				if len(pendingMessages) > 0 {
					if err := c.commitMessages(ctx, pendingMessages); err != nil {
						c.log("error", "Final commit failed", map[string]any{
							"error": err.Error(),
							"count": len(pendingMessages),
						})
					}
				}
				return
			}
			pendingMessages = append(pendingMessages, msg)

			// If we have enough messages, commit right away
			if len(pendingMessages) >= 100 {
				if err := c.commitMessages(ctx, pendingMessages); err == nil {
					pendingMessages = nil
				}
			}

		case <-commitTicker.C:
			// Time-based commit
			if len(pendingMessages) > 0 {
				if err := c.commitMessages(ctx, pendingMessages); err == nil {
					pendingMessages = nil
				}
			}

		case <-ctx.Done():
			if len(pendingMessages) > 0 {
				if err := c.commitMessages(context.Background(), pendingMessages); err != nil {
					c.log("error", "Final commit failed during shutdown", map[string]any{
						"error": err.Error(),
						"count": len(pendingMessages),
					})
				}
			}
			return

		case <-c.shutdownCh:
			if len(pendingMessages) > 0 {
				if err := c.commitMessages(context.Background(), pendingMessages); err != nil {
					c.log("error", "Final commit failed during shutdown", map[string]any{
						"error": err.Error(),
						"count": len(pendingMessages),
					})
				}
			}
			return
		}
	}
}

func (c *Consumer) commitMessages(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Find the highest offset for each partition
	offsets := make(map[int]kafka.Message)
	for _, msg := range messages {
		current, exists := offsets[msg.Partition]
		if !exists || current.Offset < msg.Offset {
			offsets[msg.Partition] = msg
		}
	}

	// Commit each partition's highest offset
	for _, msg := range offsets {
		err := c.reader.CommitMessages(ctx, msg)
		if err != nil {
			c.log("error", "Failed to commit message", map[string]any{
				"error":     err.Error(),
				"partition": msg.Partition,
				"offset":    msg.Offset,
			})
			return err
		}
	}

	c.log("debug", "Committed messages", map[string]any{
		"count": len(messages),
	})

	return nil
}
