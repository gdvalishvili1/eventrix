package pkg

import "log"

func (c *Consumer) log(level, msg string, fields map[string]any) {
	if c.options.LoggerCallback != nil {
		c.options.LoggerCallback(level, msg, fields)
	} else {
		// Default logging
		if fields != nil {
			log.Printf("[%s] %s: %v", level, msg, fields)
		} else {
			log.Printf("[%s] %s", level, msg)
		}
	}
}
