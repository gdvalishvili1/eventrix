package eventrix

import "time"

type ProcessingOptions struct {
	HandlerTimeout        time.Duration   // Max time for a handler to process a message
	MaxConcurrentMessages int             // Max number of messages to process concurrently
	RetryBackoff          []time.Duration // Backoff times for retries
	DeadLetterTopic       string          // Topic to send unprocessable messages
	CommitInterval        time.Duration   // How often to commit offsets
	MetricsCallback       metricsCallback // Function to report metrics
	LoggerCallback        loggerCallback  // Function for structured logging
	ErrorCallback         errorCallback   // Function for error handling
}

var defaultOptions = ProcessingOptions{
	HandlerTimeout:        30 * time.Second,
	MaxConcurrentMessages: 100,
	RetryBackoff:          []time.Duration{time.Second, 5 * time.Second, 15 * time.Second},
	CommitInterval:        5 * time.Second,
}
