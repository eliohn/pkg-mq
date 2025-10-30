package mq

import (
	"reflect"

	"github.com/eliohn/pkg-mq/amqpclt"
	amqp "github.com/rabbitmq/amqp091-go"
)

// PublisherConfig 用于通过配置初始化非延时发布者
type PublisherConfig struct {
	// RabbitMQ 连接
	Conn *amqp.Connection
	// 交换机名称
	Exchange string
	// 队列名称
	Queue string
	// 路由键
	RoutingKey string
}

// NewPublisher 基于配置创建并绑定一个发布者
func NewPublisher(cfg PublisherConfig) (*amqpclt.Publisher, error) {
	pub, err := amqpclt.NewPublisher(cfg.Conn, cfg.Exchange)
	if err != nil {
		return nil, err
	}
	if err = pub.BindQueue(cfg.Queue, cfg.RoutingKey); err != nil {
		return nil, err
	}
	return pub, nil
}

// SubscriberConfig 用于通过配置初始化非延时订阅者
type SubscriberConfig struct {
	// RabbitMQ 连接
	Conn *amqp.Connection
	// 交换机名称
	Exchange string
	// 队列名称
	Queue string
	// 路由键
	RoutingKey string
	// 消息类型（用于反序列化）
	MessageType reflect.Type
}

// NewSubscriber 基于配置创建并绑定一个订阅者
func NewSubscriber(cfg SubscriberConfig) (*amqpclt.Subscriber, error) {
	sub, err := amqpclt.NewSubscriber(cfg.Conn, cfg.Exchange, cfg.MessageType)
	if err != nil {
		return nil, err
	}
	if err = sub.BindQueue(cfg.Queue, cfg.RoutingKey); err != nil {
		return nil, err
	}
	return sub, nil
}
