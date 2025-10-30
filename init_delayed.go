package mq

import (
	"reflect"
	"time"

	"github.com/eliohn/pkg-mq/amqpclt"
	amqp "github.com/rabbitmq/amqp091-go"
)

// DelayedSubscriberConfig 用于通过配置初始化延迟订阅者
type DelayedSubscriberConfig struct {
	// RabbitMQ 连接
	Conn *amqp.Connection
	// 交换机名称（要求为 x-delayed-message 类型）
	Exchange string
	// 队列名称
	Queue string
	// 路由键
	RoutingKey string
	// 消息类型（用于反序列化）
	MessageType reflect.Type
}

// NewDelayedSubscriber 基于配置创建并绑定一个延迟订阅者
func NewDelayedSubscriber(cfg DelayedSubscriberConfig) (*amqpclt.DelayedSubscriber, error) {
	sub, err := amqpclt.NewDelayedSubscriber(cfg.Conn, cfg.Exchange, cfg.MessageType)
	if err != nil {
		return nil, err
	}
	if err = sub.BindQueue(cfg.Queue, cfg.RoutingKey); err != nil {
		return nil, err
	}
	return sub, nil
}

// DelayedPublisherConfig 用于通过配置初始化延迟发布者
type DelayedPublisherConfig struct {
	// RabbitMQ 连接
	Conn *amqp.Connection
	// 交换机名称（要求为 x-delayed-message 类型）
	Exchange string
	// 队列名称
	Queue string
	// 路由键
	RoutingKey string
	// 延迟时长
	Delay time.Duration
}

// NewDelayedPublisher 基于配置创建并绑定一个延迟发布者
func NewDelayedPublisher(cfg DelayedPublisherConfig) (*amqpclt.DelayedPublisher, error) {
	pub, err := amqpclt.NewDelayedPublisher(cfg.Conn, cfg.Exchange, cfg.Delay)
	if err != nil {
		return nil, err
	}
	if err = pub.BindQueue(cfg.Queue, cfg.RoutingKey); err != nil {
		return nil, err
	}
	return pub, nil
}
