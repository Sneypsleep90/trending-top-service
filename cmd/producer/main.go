package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	mathrand "math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type searchEvent struct {
	Query     string    `json:"query"`
	Timestamp time.Time `json:"timestamp"`
	SessionID string    `json:"session_id"`
	Region    string    `json:"region"`
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "search.events", "Kafka topic")
	rps := flag.Int("rps", 100, "events per second")
	queriesPath := flag.String("queries", "", "optional file with one query per line")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if *rps <= 0 {
		logger.Error("rps must be positive", slog.Int("rps", *rps))
		os.Exit(1)
	}

	queries, err := loadQueries(*queriesPath)
	if err != nil {
		logger.Error("load queries", slog.Any("error", err))
		os.Exit(1)
	}

	producer, err := newProducer(strings.Split(*brokers, ","))
	if err != nil {
		logger.Error("create producer", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("close producer", slog.Any("error", err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	random := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	interval := time.Second / time.Duration(*rps)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	sendTicker := time.NewTicker(interval)
	defer sendTicker.Stop()

	logTicker := time.NewTicker(time.Second)
	defer logTicker.Stop()

	var sent int64
	for {
		select {
		case <-ctx.Done():
			logger.Info("producer stopped", slog.Int64("sent", sent))
			return
		case <-sendTicker.C:
			event := searchEvent{
				Query:     queries[random.Intn(len(queries))],
				Timestamp: time.Now().UTC(),
				SessionID: newSessionID(),
				Region:    "RU-MOW",
			}
			if err := sendEvent(producer, *topic, event); err != nil {
				logger.Error("send event", slog.Any("error", err))
				continue
			}
			sent++
		case <-logTicker.C:
			logger.Info("events sent", slog.Int64("total", sent), slog.Int("rps", *rps))
		}
	}
}

func newProducer(brokers []string) (sarama.SyncProducer, error) {
	cleaned := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			cleaned = append(cleaned, broker)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("no kafka brokers provided")
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.ClientID = "trending-top-producer"
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(cleaned, cfg)
	if err != nil {
		return nil, fmt.Errorf("new sync producer: %w", err)
	}

	return producer, nil
}

func sendEvent(producer sarama.SyncProducer, topic string, event searchEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(event.SessionID),
		Value: sarama.ByteEncoder(payload),
	})
	if err != nil {
		return fmt.Errorf("send kafka message: %w", err)
	}

	return nil
}

func loadQueries(path string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		return builtinQueries(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read queries file: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	queries := make([]string, 0, len(lines))
	for _, line := range lines {
		query := strings.TrimSpace(strings.ToLower(line))
		if query != "" {
			queries = append(queries, query)
		}
	}
	if len(queries) == 0 {
		return nil, fmt.Errorf("queries file is empty")
	}

	return queries, nil
}

func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func builtinQueries() []string {
	return []string{
		"кроссовки найк",
		"айфон 15",
		"платье летнее",
		"футболка мужская",
		"наушники беспроводные",
		"сумка женская",
		"ноутбук игровой",
		"чехол на айфон",
		"джинсы женские",
		"пылесос вертикальный",
		"детские кроссовки",
		"куртка демисезонная",
		"смарт часы",
		"крем для лица",
		"рюкзак городской",
		"постельное белье",
		"телевизор 55",
		"робот пылесос",
		"платье вечернее",
		"шампунь",
		"кеды белые",
		"зарядка type c",
		"мышь беспроводная",
		"клавиатура механическая",
		"толстовка мужская",
		"костюм спортивный",
		"ботинки зимние",
		"пуховик женский",
		"серьги серебро",
		"кольцо золотое",
		"духи женские",
		"кофемашина",
		"микроволновка",
		"стул компьютерный",
		"матрас 160х200",
		"шторы блэкаут",
		"лампа настольная",
		"конструктор лего",
		"коляска детская",
		"подгузники",
		"корм для кошек",
		"велосипед",
		"самокат детский",
		"гель для душа",
		"электрическая зубная щетка",
		"фен для волос",
		"утюг",
		"сковорода",
		"кастрюля",
		"термос",
	}
}
