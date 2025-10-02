package rocketmq

import (
	"context"
	"dianping-go/config"
	"encoding/json"
	"log/slog"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

var (
	consumerClient rocketmq.PushConsumer
	orderHandler   func(ctx context.Context, voucherId, userId, orderId int64) error
)

func SetOrderHandler(handler func(ctx context.Context, voucherId, userId, orderId int64) error) {
	orderHandler = handler
}

func InitConsumer() error {
	var err error
	consumerClient, err = rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{config.RocketMQOption.NameServer}),
		consumer.WithGroupName(config.RocketMQOption.ConsumerGroup),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset),
		consumer.WithMaxReconsumeTimes(config.RocketMQOption.MaxReconsumeTimes),
	)
	if err != nil {
		slog.Error("Failed to create RocketMQ consumer", "err", err)
		return err
	}

	// 注册消息监听器
	err = consumerClient.Subscribe(config.RocketMQOption.Topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			var orderMsg OrderMessage
			err := json.Unmarshal(msg.Body, &orderMsg)
			if err != nil {
				slog.Error("Failed to unmarshal order message", "err", err, "messageId", msg.MsgId)
				continue
			}

			// 处理订单
			if orderHandler != nil {
				err = orderHandler(ctx, int64(orderMsg.VoucherId), int64(orderMsg.UserId), int64(orderMsg.OrderId))
				if err != nil {
					slog.Error("Failed to process order", "err", err, "voucherId", orderMsg.VoucherId, "userId", orderMsg.UserId, "orderId", orderMsg.OrderId)
					// 如果处理失败，返回稍后重试
					return consumer.ConsumeRetryLater, nil
				}
			} else {
				slog.Error("Order handler not set", "voucherId", orderMsg.VoucherId, "userId", orderMsg.UserId, "orderId", orderMsg.OrderId)
				return consumer.ConsumeRetryLater, nil
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		slog.Error("Failed to subscribe topic", "err", err)
		return err
	}

	// 启动消费者
	err = consumerClient.Start()
	if err != nil {
		slog.Error("Failed to start RocketMQ consumer", "err", err)
		return err
	}

	slog.Info("RocketMQ consumer started successfully", "topic", config.RocketMQOption.Topic, "consumerGroup", config.RocketMQOption.ConsumerGroup)
	return nil
}

func ShutdownConsumer() error {
	if consumerClient != nil {
		err := consumerClient.Shutdown()
		if err != nil {
			slog.Error("Failed to shutdown RocketMQ consumer", "err", err)
			return err
		}
		slog.Info("RocketMQ consumer shutdown successfully")
	}
	return nil
}
