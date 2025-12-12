package mq

import (
	"reflect"

	"github.com/eliohn/pkg-mq/amqpclt"
	amqp "github.com/rabbitmq/amqp091-go"
)

// BroadcastPublisherConfig 用于通过配置初始化广播发布者
type BroadcastPublisherConfig struct {
	// RabbitMQ 连接
	Conn *amqp.Connection
	// 交换机名称（fanout 类型）
	Exchange string
	// 队列名称（可选，用于绑定队列）
	Queue string
	// 路由键（fanout 类型会忽略，但为了保持接口一致性，仍然接受）
	RoutingKey string
	// 队列配置（可选，为 nil 时使用默认值）
	QueueConfig *amqpclt.QueueConfig
}

// NewBroadcastPublisher 基于配置创建并绑定一个广播发布者
func NewBroadcastPublisher(cfg BroadcastPublisherConfig) (*amqpclt.BroadcastPublisher, error) {
	pub, err := amqpclt.NewBroadcastPublisher(cfg.Conn, cfg.Exchange)
	if err != nil {
		return nil, err
	}
	// 如果提供了队列名称，则绑定队列
	if cfg.Queue != "" {
		if err = pub.BindQueue(cfg.Queue, cfg.RoutingKey, cfg.QueueConfig); err != nil {
			return nil, err
		}
	}
	return pub, nil
}

// BroadcastSubscriberConfig 用于通过配置初始化广播订阅者
type BroadcastSubscriberConfig struct {
	// RabbitMQ 连接
	Conn *amqp.Connection
	// 交换机名称（fanout 类型）
	Exchange string
	// 队列名称（每个订阅者通常需要自己的队列）
	Queue string
	// 路由键（fanout 类型会忽略，但为了保持接口一致性，仍然接受）
	RoutingKey string
	// 消息类型（用于反序列化）
	MessageType reflect.Type
	// 队列配置（可选，为 nil 时使用默认值）
	QueueConfig *amqpclt.QueueConfig
}

// NewBroadcastSubscriber 基于配置创建并绑定一个广播订阅者
func NewBroadcastSubscriber(cfg BroadcastSubscriberConfig) (*amqpclt.BroadcastSubscriber, error) {
	sub, err := amqpclt.NewBroadcastSubscriber(cfg.Conn, cfg.Exchange, cfg.MessageType)
	if err != nil {
		return nil, err
	}
	if err = sub.BindQueue(cfg.Queue, cfg.RoutingKey, cfg.QueueConfig); err != nil {
		return nil, err
	}
	return sub, nil
}
