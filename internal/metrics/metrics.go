package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	registry *prometheus.Registry

	eventsConsumed     prometheus.Counter
	fraudDropped       prometheus.Counter
	stoplistDropped    prometheus.Counter
	kafkaErrors        prometheus.Counter
	storeWrites        prometheus.Counter
	topRequestDuration prometheus.Histogram
	storeUniqueQueries prometheus.Gauge
}

func New(registry *prometheus.Registry) *Metrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	factory := promauto.With(registry)

	return &Metrics{
		registry: registry,
		eventsConsumed: factory.NewCounter(prometheus.CounterOpts{
			Name: "trending_events_consumed_total",
			Help: "Total Kafka events consumed.",
		}),
		fraudDropped: factory.NewCounter(prometheus.CounterOpts{
			Name: "trending_fraud_dropped_total",
			Help: "Events dropped by fraud detector.",
		}),
		stoplistDropped: factory.NewCounter(prometheus.CounterOpts{
			Name: "trending_stoplist_dropped_total",
			Help: "Events dropped by stop list.",
		}),
		kafkaErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "trending_kafka_errors_total",
			Help: "Kafka consumer errors.",
		}),
		storeWrites: factory.NewCounter(prometheus.CounterOpts{
			Name: "trending_store_writes_total",
			Help: "Events written to the store.",
		}),
		topRequestDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "trending_top_request_duration_seconds",
			Help:    "Duration of top endpoint requests.",
			Buckets: prometheus.DefBuckets,
		}),
		storeUniqueQueries: factory.NewGauge(prometheus.GaugeOpts{
			Name: "trending_store_unique_queries",
			Help: "Current number of unique queries in window.",
		}),
	}
}

func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return prometheus.NewRegistry()
	}

	return m.registry
}

func (m *Metrics) IncEventsConsumed() {
	if m != nil {
		m.eventsConsumed.Inc()
	}
}

func (m *Metrics) IncFraudDropped() {
	if m != nil {
		m.fraudDropped.Inc()
	}
}

func (m *Metrics) IncStoplistDropped() {
	if m != nil {
		m.stoplistDropped.Inc()
	}
}

func (m *Metrics) IncKafkaErrors() {
	if m != nil {
		m.kafkaErrors.Inc()
	}
}

func (m *Metrics) IncStoreWrites() {
	if m != nil {
		m.storeWrites.Inc()
	}
}

func (m *Metrics) ObserveTopRequest(duration time.Duration) {
	if m != nil {
		m.topRequestDuration.Observe(duration.Seconds())
	}
}

func (m *Metrics) SetStoreUniqueQueries(count int) {
	if m != nil {
		m.storeUniqueQueries.Set(float64(count))
	}
}
