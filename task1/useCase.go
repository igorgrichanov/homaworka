package task1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type Order struct {
	ID     int
	UserID int
	Amount float64
	Status string
}

type OrderEvent struct {
	OrderID int
	UserID  int
	Amount  float64
	Status  string
}

type OrderRepository interface {
	CreateOrder(ctx context.Context, order *Order) error
	UpdateOrderStatus(ctx context.Context, orderID int, status string) error
	SaveOutboxMessage(ctx context.Context, topic string, payload []byte) error
}

type UseCase struct {
	repo OrderRepository
	tx   TxManager
}

func NewUseCase(repo OrderRepository, tx TxManager) *UseCase {
	return &UseCase{
		repo: repo,
		tx:   tx,
	}
}

func (uc *UseCase) CreateOrder(ctx context.Context, userID int, amount float64) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}
	return uc.tx.Do(ctx, func(tx Tx) error {
		ctx := tx.NewContext()
		order := &Order{
			UserID: userID,
			Amount: amount,
			Status: "created",
		}

		if err := uc.repo.CreateOrder(ctx, order); err != nil {
			return fmt.Errorf("failed to create order: %v", err)
		}

		// Отправляем событие в outbox
		event := &OrderEvent{
			OrderID: order.ID,
			UserID:  userID,
			Amount:  amount,
			Status:  "created",
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %v", err)
		}

		if err := uc.repo.SaveOutboxMessage(ctx, "order_created", payload); err != nil {
			return fmt.Errorf("failed to save outbox message: %v", err)
		}
		return nil
	})
}
