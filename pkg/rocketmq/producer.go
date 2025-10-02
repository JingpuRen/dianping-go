package rocketmq

import (
	"context"
	"dianping-go/config"
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"log/slog"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type OrderMessage struct {
	VoucherId int `json:"voucherId"`
	UserId    int `json:"userId"`
	OrderId   int `json:"orderId"`
}

var (
	producerClient rocketmq.Producer
)

func InitProducer() error {
	var err error
	producerClient, err = rocketmq.NewProducer(
		producer.WithNameServer([]string{config.RocketMQOption.NameServer}),
		producer.WithGroupName(config.RocketMQOption.ProducerGroup),
		producer.WithRetry(2),
	)
	if err != nil {
		slog.Error("Failed to create RocketMQ producer", "err", err)
		return err
	}

	err = producerClient.Start()
	if err != nil {
		slog.Error("Failed to start RocketMQ producer", "err", err)
		return err
	}

	slog.Info("RocketMQ producer started successfully")
	return nil
}

func SendOrderMessage(ctx context.Context, voucherId, userId, orderId int) error {
	message := OrderMessage{
		VoucherId: voucherId,
		UserId:    userId,
		OrderId:   orderId,
	}

	body, err := json.Marshal(message)
	if err != nil {
		slog.Error("Failed to marshal order message", "err", err)
		return err
	}

	msg := primitive.NewMessage(config.RocketMQOption.Topic, body)

	result, err := producerClient.SendSync(ctx, msg)
	if err != nil {
		slog.Error("Failed to send order message", "err", err)
		return err
	}

	slog.Info("Order message sent successfully", "messageId", result.MsgID, "voucherId", voucherId, "userId", userId, "orderId", orderId)
	return nil
}

func ShutdownProducer() error {
	if producerClient != nil {
		err := producerClient.Shutdown()
		if err != nil {
			slog.Error("Failed to shutdown RocketMQ producer", "err", err)
			return err
		}
		slog.Info("RocketMQ producer shutdown successfully")
	}
	return nil
}
