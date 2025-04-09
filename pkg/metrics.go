package pkg

type (
	metricsCallback func(metric string, value float64, tags map[string]string)
	loggerCallback  func(level string, msg string, fields map[string]any)
	errorCallback   func(err error, eventType string, key string, attempt int)
)

// IsHealthy returns the health status of the consumer
func (c *Consumer) IsHealthy() bool {
	c.healthLock.RLock()
	defer c.healthLock.RUnlock()
	return c.healthy
}

func (c *Consumer) setHealthy(healthy bool) {
	c.healthLock.Lock()
	defer c.healthLock.Unlock()
	c.healthy = healthy
}

func (c *Consumer) incrementCounter() {
	c.counterLock.Lock()
	defer c.counterLock.Unlock()
	c.messageCounter++

	// Report metrics
	if c.options.MetricsCallback != nil {
		c.options.MetricsCallback("messages_processed_total", float64(c.messageCounter), nil)
	}
}

func (c *Consumer) incrementErrorCounter(eventType string) {
	c.counterLock.Lock()
	defer c.counterLock.Unlock()
	c.errorCounter[eventType]++

	// Report metrics
	if c.options.MetricsCallback != nil {
		c.options.MetricsCallback("message_errors", float64(c.errorCounter[eventType]), map[string]string{
			"event_type": eventType,
		})
	}
}
