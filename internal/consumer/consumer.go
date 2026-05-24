package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"

	"github.com/Sneypsleep90/trending-top/internal/config"
	"github.com/Sneypsleep90/trending-top/internal/metrics"
	"github.com/Sneypsleep90/trending-top/internal/store"
)

// Consumer reads Kafka search events and writes accepted queries to the store.
type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	store   store.Store
	fraud   *FraudDetector
	metrics *metrics.Metrics
	logger  *slog.Logger
}

// NewConsumer creates a Sarama consumer group.
func NewConsumer(
	cfg config.Config,
	st store.Store,
	fraud *FraudDetector,
	metricSet *metrics.Metrics,
	logger *slog.Logger,
) (*Consumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_8_0_0
	saramaCfg.ClientID = "trending-top"
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = true
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}

	group, err := sarama.NewConsumerGroup(cfg.KafkaBrokerList(), cfg.KafkaGroupID, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("consumer.NewConsumer: %w", err)
	}

	return &Consumer{
		group:   group,
		topic:   cfg.KafkaTopic,
		store:   st,
		fraud:   fraud,
		metrics: metricSet,
		logger:  logger,
	}, nil
}

// Run consumes the configured topic until ctx is canceled.
func (c *Consumer) Run(ctx context.Context) {
	defer func() {
		if err := c.group.Close(); err != nil {
			c.logger.Error("close kafka consumer group", slog.Any("error", err))
		}
	}()

	handler := &consumerGroupHandler{consumer: c}
	topics := []string{c.topic}

	for {
		if err := c.group.Consume(ctx, topics, handler); err != nil {
			if ctx.Err() != nil || errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}

			c.metrics.IncKafkaErrors()
			c.logger.Error("kafka consume error", slog.Any("error", err))

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}

		if ctx.Err() != nil {
			return
		}
	}
}

// Close closes the underlying consumer group.
func (c *Consumer) Close() error {
	if err := c.group.Close(); err != nil {
		return fmt.Errorf("consumer.Close: %w", err)
	}

	return nil
}

type consumerGroupHandler struct {
	consumer *Consumer
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.consumer.logger.Info(
		"kafka consumer group setup",
		slog.String("member_id", session.MemberID()),
		slog.Int64("generation_id", int64(session.GenerationID())),
	)

	return nil
}

func (h *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.consumer.logger.Info(
		"kafka consumer group cleanup",
		slog.String("member_id", session.MemberID()),
		slog.Int64("generation_id", int64(session.GenerationID())),
	)

	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			h.handleMessage(session, message)
		}
	}
}

func (h *consumerGroupHandler) handleMessage(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) {
	event, err := DecodeSearchEvent(message.Value)
	if err != nil {
		h.consumer.metrics.IncKafkaErrors()
		h.consumer.logger.Warn(
			"skip invalid kafka event",
			slog.Any("error", err),
			slog.String("topic", message.Topic),
			slog.Int64("partition", int64(message.Partition)),
			slog.Int64("offset", message.Offset),
		)
		session.MarkMessage(message, "")

		return
	}

	if h.consumer.fraud.IsFraud(event.SessionID, event.Query) {
		h.consumer.metrics.IncFraudDropped()
		session.MarkMessage(message, "")

		return
	}

	if err := h.consumer.store.Add(session.Context(), event.Query); err != nil {
		h.consumer.metrics.IncKafkaErrors()
		h.consumer.logger.Error(
			"store kafka event",
			slog.Any("error", err),
			slog.String("query", event.Query),
			slog.String("topic", message.Topic),
			slog.Int64("partition", int64(message.Partition)),
			slog.Int64("offset", message.Offset),
		)

		return
	}

	h.consumer.metrics.IncEventsConsumed()
	session.MarkMessage(message, "")
}

// EventSource exposes a stream of already decoded search events.
type EventSource interface {
	Events() <-chan SearchEvent
}

// MockSource is an EventSource implementation backed by a slice.
type MockSource struct {
	events chan SearchEvent
}

// NewMockSource creates a source that emits the provided events once.
func NewMockSource(events []SearchEvent) *MockSource {
	ch := make(chan SearchEvent, len(events))
	for _, event := range events {
		ch <- event
	}
	close(ch)

	return &MockSource{events: ch}
}

// Events returns the mock event channel.
func (s *MockSource) Events() <-chan SearchEvent {
	return s.events
}
