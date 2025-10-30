package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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
	Queue      string `json:"queue"`
	RoutingKey string `json:"routing_key"`
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
		cfg.AmqpURL = os.Getenv("AMQP_URL")
		if cfg.AmqpURL == "" {
			cfg.AmqpURL = "amqp://guest:guest@localhost:5672/"
		}
		if cfg.Exchange == "" {
			cfg.Exchange = "demo.normal.exchange"
		}
		if cfg.Queue == "" {
			cfg.Queue = "demo.normal.queue"
		}
		if cfg.RoutingKey == "" {
			cfg.RoutingKey = "demo.normal.rk"
		}
	}

	conn, err := amqp.Dial(cfg.AmqpURL)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 订阅者
	sub, err := mq.NewSubscriber(mq.SubscriberConfig{
		Conn:        conn,
		Exchange:    cfg.Exchange,
		Queue:       cfg.Queue,
		RoutingKey:  cfg.RoutingKey,
		MessageType: reflect.TypeOf(ExampleMsg{}),
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh, err := sub.Subscribe(ctx)
	if err != nil {
		panic(err)
	}

	done := make(chan struct{})
	go func() {
		v := <-msgCh
		if v != nil {
			if m, ok := v.(*ExampleMsg); ok {
				fmt.Printf("收到普通消息: ID=%d, Text=%s\n", m.ID, m.Text)
			} else {
				fmt.Printf("收到未知类型消息: %T\n", v)
			}
		}
		close(done)
	}()

	// 发布者（无延时）
	pub, err := mq.NewPublisher(mq.PublisherConfig{
		Conn:       conn,
		Exchange:   cfg.Exchange,
		Queue:      cfg.Queue,
		RoutingKey: cfg.RoutingKey,
	})
	if err != nil {
		panic(err)
	}

	if err := pub.Publish(context.Background(), ExampleMsg{ID: int(time.Now().Unix()), Text: "hello normal"}); err != nil {
		panic(err)
	}
	fmt.Println("已发送普通消息，等待接收...")

	<-done
	fmt.Println("演示结束")
}
