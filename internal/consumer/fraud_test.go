package consumer

import (
	"testing"
	"time"
)

func TestFraudDetector_IsFraud(t *testing.T) {
	detector := NewFraudDetector(2, time.Minute)

	if detector.IsFraud("session-1", "кроссовки найк") {
		t.Fatal("first event must not be fraud")
	}
	if detector.IsFraud("session-1", "кроссовки найк") {
		t.Fatal("second event must not be fraud")
	}
	if !detector.IsFraud("session-1", "кроссовки найк") {
		t.Fatal("third event must be fraud")
	}
	if detector.IsFraud("session-1", "айфон 15") {
		t.Fatal("different query must have its own counter")
	}
}

func TestFraudDetector_WindowReset(t *testing.T) {
	detector := NewFraudDetector(1, time.Minute)

	if detector.IsFraud("session-1", "кроссовки найк") {
		t.Fatal("first event must not be fraud")
	}
	if !detector.IsFraud("session-1", "кроссовки найк") {
		t.Fatal("second event must be fraud before expiration")
	}

	value, ok := detector.sessions.Load("session-1")
	if !ok {
		t.Fatal("expected session entry")
	}
	entry := value.(*sessionEntry)
	entry.mu.Lock()
	entry.expiresAt = time.Now().Add(-time.Second)
	entry.mu.Unlock()

	if detector.IsFraud("session-1", "кроссовки найк") {
		t.Fatal("event after expiration must not be fraud")
	}
}
