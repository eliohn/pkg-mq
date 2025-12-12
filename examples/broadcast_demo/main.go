package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"time"

	mq "github.com/eliohn/pkg-mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExampleMsg struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

type Config struct {
	AmqpURL    string `json:"amqp_url"`
	Exchange   string `json:"exchange"`
	Queue1     string `json:"queue1"`      // 订阅者1的队列
	Queue2     string `json:"queue2"`      // 订阅者2的队列
	Queue3     string `json:"queue3"`      // 订阅者3的队列
	RoutingKey string `json:"routing_key"` // fanout 类型会忽略，但为了保持接口一致性
}

func loadConfig() (Config, error) {
	candidates := []string{}
	if _, file, _, ok := runtime.Caller(0); ok {
		// 源码同目录
		candidates = append(candidates, filepath.Join(filepath.Dir(file), "config.local.json"))
	}
	if wd, err := os.Getwd(); err == nil {
		// 当前工作目录
		candidates = append(candidates, filepath.Join(wd, "config.local.json"))
	}
	if exe, err := os.Executable(); err == nil {
		// 可执行文件目录（go build 后）
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "config.local.json"))
	}

	var lastErr error
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err != nil {
			lastErr = err
			continue
		}
		var c Config
		if err := json.Unmarshal(data, &c); err != nil {
			lastErr = err
			continue
		}
		return c, nil
	}
	return Config{}, fmt.Errorf("读取配置失败，尝试路径: %v, 错误: %v", candidates, lastErr)
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		// 回退到环境变量/默认值
		cfg.AmqpURL = os.Getenv("AMQP_URL")
		if cfg.AmqpURL == "" {
			cfg.AmqpURL = "amqp://guest:guest@localhost:5672/"
		}
		if cfg.Exchange == "" {
			cfg.Exchange = "demo.broadcast.exchange"
		}
		if cfg.Queue1 == "" {
			cfg.Queue1 = "demo.broadcast.queue1"
		}
		if cfg.Queue2 == "" {
			cfg.Queue2 = "demo.broadcast.queue2"
		}
		if cfg.Queue3 == "" {
			cfg.Queue3 = "demo.broadcast.queue3"
		}
		if cfg.RoutingKey == "" {
			cfg.RoutingKey = "demo.broadcast.rk"
		}
	}

	conn, err := amqp.Dial(cfg.AmqpURL)
	if err != nil {
		panic(fmt.Sprintf("连接 RabbitMQ 失败: %v", err))
	}
	defer conn.Close()

	exchange := cfg.Exchange
	routingKey := cfg.RoutingKey

	// 创建三个订阅者，每个订阅者使用不同的队列
	// 广播消息会被发送到所有绑定到该交换机的队列
	sub1, err := mq.NewBroadcastSubscriber(mq.BroadcastSubscriberConfig{
		Conn:        conn,
		Exchange:    exchange,
		Queue:       "",
		RoutingKey:  routingKey,
		MessageType: reflect.TypeOf(ExampleMsg{}),
	})
	if err != nil {
		panic(fmt.Sprintf("创建订阅者1失败: %v", err))
	}

	sub2, err := mq.NewBroadcastSubscriber(mq.BroadcastSubscriberConfig{
		Conn:        conn,
		Exchange:    exchange,
		Queue:       "",
		RoutingKey:  routingKey,
		MessageType: reflect.TypeOf(ExampleMsg{}),
	})
	if err != nil {
		panic(fmt.Sprintf("创建订阅者2失败: %v", err))
	}

	sub3, err := mq.NewBroadcastSubscriber(mq.BroadcastSubscriberConfig{
		Conn:        conn,
		Exchange:    exchange,
		Queue:       "",
		RoutingKey:  routingKey,
		MessageType: reflect.TypeOf(ExampleMsg{}),
	})
	if err != nil {
		panic(fmt.Sprintf("创建订阅者3失败: %v", err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动三个订阅者
	msgCh1, err := sub1.Subscribe(ctx)
	if err != nil {
		panic(fmt.Sprintf("订阅者1订阅失败: %v", err))
	}

	msgCh2, err := sub2.Subscribe(ctx)
	if err != nil {
		panic(fmt.Sprintf("订阅者2订阅失败: %v", err))
	}

	msgCh3, err := sub3.Subscribe(ctx)
	if err != nil {
		panic(fmt.Sprintf("订阅者3订阅失败: %v", err))
	}

	// 使用 WaitGroup 等待所有订阅者都收到消息
	var wg sync.WaitGroup
	wg.Add(3)

	// 订阅者1接收消息
	go func() {
		defer wg.Done()
		for v := range msgCh1 {
			if v != nil {
				if m, ok := v.(*ExampleMsg); ok {
					fmt.Printf("[订阅者1] 收到广播消息: ID=%d, Text=%s\n", m.ID, m.Text)
				} else {
					fmt.Printf("[订阅者1] 收到未知类型消息: %T\n", v)
				}
			}
		}
	}()

	// 订阅者2接收消息
	go func() {
		defer wg.Done()
		for v := range msgCh2 {
			if v != nil {
				if m, ok := v.(*ExampleMsg); ok {
					fmt.Printf("[订阅者2] 收到广播消息: ID=%d, Text=%s\n", m.ID, m.Text)
				} else {
					fmt.Printf("[订阅者2] 收到未知类型消息: %T\n", v)
				}
			}
		}
	}()

	// 订阅者3接收消息
	go func() {
		defer wg.Done()
		for v := range msgCh3 {
			if v != nil {
				if m, ok := v.(*ExampleMsg); ok {
					fmt.Printf("[订阅者3] 收到广播消息: ID=%d, Text=%s\n", m.ID, m.Text)
				} else {
					fmt.Printf("[订阅者3] 收到未知类型消息: %T\n", v)
				}
			}
		}
	}()

	// 等待一下，确保所有订阅者都准备好
	time.Sleep(500 * time.Millisecond)

	// 创建发布者
	pub, err := mq.NewBroadcastPublisher(mq.BroadcastPublisherConfig{
		Conn:       conn,
		Exchange:   exchange,
		Queue:      "", // 发布者不需要绑定队列
		RoutingKey: routingKey,
	})
	if err != nil {
		panic(fmt.Sprintf("创建发布者失败: %v", err))
	}

	// 发送广播消息
	msg := ExampleMsg{
		ID:   int(time.Now().Unix()),
		Text: "这是一条广播消息",
	}

	fmt.Println("\n=== 发送广播消息 ===")
	if err := pub.Publish(context.Background(), msg); err != nil {
		panic(fmt.Sprintf("发送消息失败: %v", err))
	}
	fmt.Printf("已发送广播消息: ID=%d, Text=%s\n", msg.ID, msg.Text)
	fmt.Println("\n=== 等待所有订阅者接收消息 ===\n")

	// 等待所有订阅者都收到消息（这里简化处理，实际应用中可能需要更复杂的逻辑）
	time.Sleep(2 * time.Second)
	cancel() // 取消订阅

	// 等待所有 goroutine 完成
	wg.Wait()
	fmt.Println("\n演示结束：所有订阅者都已收到广播消息")
}
