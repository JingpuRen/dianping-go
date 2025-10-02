package main

import (
	"context"
	"dianping-go/config"
	"dianping-go/db"
	"dianping-go/pkg/logger"
	"dianping-go/pkg/rocketmq"
	"dianping-go/router"
	"dianping-go/service/order"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
)

func init() {
	configPath := pflag.StringP("config", "c", "configs/config.yaml", "config file path")
	pflag.Parse()

	config.InitConfig(*configPath)      //初始化配置
	logger.InitLogger(config.LogOption) //初始化日志
	slog.Info("config init success")
	var err error
	//初始化数据库
	db.DBEngine, err = db.NewMySQL(config.MysqlOption)
	if err != nil {
		panic(err)
	}
	db.RedisDb, err = db.NewRedisClient(config.RedisOption)
	if err != nil {
		panic(err)
	}

	// 初始化RocketMQ生产者和消费者
	err = rocketmq.InitProducer()
	if err != nil {
		panic(err)
	}

	// 设置订单处理器
	rocketmq.SetOrderHandler(func(ctx context.Context, voucherId, userId, orderId int64) error {
		return order.CreateOrder(int(voucherId), int(userId), int(orderId))
	})

	err = rocketmq.InitConsumer()
	if err != nil {
		panic(err)
	}
}

func main() {
	r := router.NewRouter()
	//创建HTTP服务器
	server := http.Server{
		Addr:         ":" + config.ServerOption.HttpPort,
		ReadTimeout:  config.ServerOption.ReadTimeout,
		WriteTimeout: config.ServerOption.WriteTimeout,
		Handler:      r,
	}

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // syscall.SIGKILL是无法捕捉的
	<-quit
	fmt.Println("shutdown server...")

	//创建超时上下文，Shutdown可以让未处理的连接在这个时间内关闭
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		panic(err)
	}

	// 优雅关闭RocketMQ
	rocketmq.ShutdownProducer()
	rocketmq.ShutdownConsumer()

	fmt.Println("server shutdown success")
}
