package task1

import (
	"context"
	"log/slog"
	"time"
)

/*
	CREATE TABLE IF NOT EXISTS events(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	topic TEXT NOT NULL,
	payload TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now(),
	reserved_to TIMESTAMPTZ DEFAULT NULL
);
*/

type OutboxRepository interface {
	GetMessages(ctx context.Context, topic string, size int) ([]*OrderEvent, error) // читаем батч
	SetMessageSent(ctx context.Context, sentIDs []int) error                        // чистим батч из outbox
}

type MessageBroker interface {
	PublishOrderCreated(ctx context.Context, event *OrderEvent) error
}

type Sender struct {
	repo   OutboxRepository
	broker MessageBroker
	logger *slog.Logger
}

func NewSender(repo OutboxRepository, broker MessageBroker, logger *slog.Logger) *Sender {
	return &Sender{
		repo:   repo,
		broker: broker,
		logger: logger,
	}
}

func (s *Sender) StartProcessEvents(ctx context.Context, handlePeriod time.Duration) error {
	const op = "eventSender.StartProcessEvents"
	log := s.logger.With(slog.String("op", op))
	ticker := time.NewTicker(handlePeriod)

	for {
		select {
		case <-ctx.Done():
			log.Info("stopping event processing")
			return ctx.Err()
		case <-ticker.C:
		}

		events, err := s.repo.GetMessages(ctx, "default", 100)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		if len(events) == 0 {
			log.Debug("no events received")
		}

		// слайс для хранения orderID отправленных заказов для их удаления из outbox
		sentIDs := make([]int, 100)
		for _, event := range events {
			err = s.broker.PublishOrderCreated(ctx, event)
			if err != nil {
				log.Error(err.Error())
				continue
			}
			sentIDs = append(sentIDs, event.OrderID)
		}
		err = s.repo.SetMessageSent(ctx, sentIDs)
		if err != nil {
			log.Error(err.Error())
		}
	}
}

type MockBroker struct {
	logger *slog.Logger
}

func (mb *MockBroker) PublishOrderCreated(ctx context.Context, event *OrderEvent) error {
	const op = "eventSender.SendMesage"
	log := mb.logger.With(slog.String("op", op))

	log.Info("sending message", slog.Any("event", event))
	return nil
}
