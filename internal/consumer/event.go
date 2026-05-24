package consumer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SearchEvent is the Kafka payload describing one marketplace search.
type SearchEvent struct {
	Query     string    `json:"query"`
	Timestamp time.Time `json:"timestamp"`
	SessionID string    `json:"session_id"`
	Region    string    `json:"region"`
}

// DecodeSearchEvent decodes, normalizes, and validates a Kafka payload.
func DecodeSearchEvent(payload []byte) (SearchEvent, error) {
	var event SearchEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return SearchEvent{}, fmt.Errorf("consumer.DecodeSearchEvent: %w", err)
	}

	event.Query = NormalizeQuery(event.Query)
	event.SessionID = strings.TrimSpace(event.SessionID)
	event.Region = strings.TrimSpace(event.Region)

	if err := event.Validate(); err != nil {
		return SearchEvent{}, fmt.Errorf("consumer.DecodeSearchEvent: %w", err)
	}

	return event, nil
}

// NormalizeQuery normalizes a user search query.
func NormalizeQuery(query string) string {
	return strings.TrimSpace(strings.ToLower(query))
}

// Validate checks that the event has the required fields.
func (e SearchEvent) Validate() error {
	if e.Query == "" {
		return fmt.Errorf("query is empty")
	}
	if e.SessionID == "" {
		return fmt.Errorf("session_id is empty")
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is empty")
	}

	return nil
}
