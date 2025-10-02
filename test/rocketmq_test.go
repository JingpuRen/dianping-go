package test

import (
	"context"
	"dianping-go/pkg/rocketmq"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 用于跟踪消费者是否收到消息
var (
	messageReceived   bool
	receivedVoucherId int
	receivedUserId    int
	receivedOrderId   int
	mu                sync.Mutex
)

// 测试用的订单处理函数
func testOrderHandler(ctx context.Context, voucherId, userId, orderId int64) error {
	mu.Lock()
	defer mu.Unlock()

	messageReceived = true
	receivedVoucherId = int(voucherId)
	receivedUserId = int(userId)
	receivedOrderId = int(orderId)

	fmt.Printf("✅ 消费者收到消息: voucherId=%d, userId=%d, orderId=%d\n", voucherId, userId, orderId)
	return nil
}

func TestRocketMQProducer(t *testing.T) {
	// 初始化RocketMQ生产者
	err := rocketmq.InitProducer()
	if err != nil {
		t.Errorf("Failed to init RocketMQ producer: %v", err)
		return
	}
	defer rocketmq.ShutdownProducer()

	ctx := context.Background()

	// 发送测试消息
	err = rocketmq.SendOrderMessage(ctx, 1, 1001, 12345)
	if err != nil {
		t.Errorf("Failed to send order message: %v", err)
		return
	}

	fmt.Println("✅ 生产者：订单消息发送成功")

	// 等待消费者处理
	time.Sleep(2 * time.Second)
}

func TestRocketMQConsumer(t *testing.T) {
	// 设置订单处理函数
	rocketmq.SetOrderHandler(testOrderHandler)

	// 初始化RocketMQ消费者
	err := rocketmq.InitConsumer()
	if err != nil {
		t.Errorf("Failed to init RocketMQ consumer: %v", err)
		return
	}
	defer rocketmq.ShutdownConsumer()

	fmt.Println("✅ 消费者：RocketMQ消费者启动成功")

	// 让消费者运行一段时间来接收消息
	time.Sleep(5 * time.Second)
}

func TestRocketMQEndToEnd(t *testing.T) {
	fmt.Println("🚀 开始端到端RocketMQ测试...")

	// 设置订单处理函数
	rocketmq.SetOrderHandler(testOrderHandler)

	// 初始化消费者（先启动消费者，确保能收到消息）
	err := rocketmq.InitConsumer()
	if err != nil {
		t.Errorf("Failed to init RocketMQ consumer: %v", err)
		return
	}
	defer rocketmq.ShutdownConsumer()

	// 给消费者一点时间来启动
	time.Sleep(1 * time.Second)

	// 初始化生产者
	err = rocketmq.InitProducer()
	if err != nil {
		t.Errorf("Failed to init RocketMQ producer: %v", err)
		return
	}
	defer rocketmq.ShutdownProducer()

	// 重置消息接收状态
	mu.Lock()
	messageReceived = false
	mu.Unlock()

	ctx := context.Background()

	// 发送测试消息
	testVoucherId := 999
	testUserId := 8888
	testOrderId := 77777

	fmt.Printf("📤 生产者：发送测试消息 (voucherId=%d, userId=%d, orderId=%d)\n", testVoucherId, testUserId, testOrderId)

	err = rocketmq.SendOrderMessage(ctx, testVoucherId, testUserId, testOrderId)
	if err != nil {
		t.Errorf("Failed to send order message: %v", err)
		return
	}

	// 等待消息被消费
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Errorf("❌ 测试失败：超时未收到消息")
			return
		case <-ticker.C:
			mu.Lock()
			if messageReceived {
				if receivedVoucherId == testVoucherId && receivedUserId == testUserId && receivedOrderId == testOrderId {
					fmt.Println("✅ 端到端测试成功：消息正确发送并被消费")
					mu.Unlock()
					return
				} else {
					t.Errorf("❌ 消息内容不匹配：期望(voucherId=%d, userId=%d, orderId=%d)，实际(voucherId=%d, userId=%d, orderId=%d)",
						testVoucherId, testUserId, testOrderId, receivedVoucherId, receivedUserId, receivedOrderId)
					mu.Unlock()
					return
				}
			}
			mu.Unlock()
		}
	}
}
