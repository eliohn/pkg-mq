package amqpclt

import (
	"context"
	"fmt"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueConfig 队列配置，用于声明队列时的参数
// 如果字段为 nil，则使用默认值
type QueueConfig struct {
	// Durable 队列是否持久化，默认 true
	Durable *bool
	// AutoDelete 队列是否自动删除，默认 false
	AutoDelete *bool
	// Exclusive 队列是否独占，默认 false
	Exclusive *bool
	// NoWait 是否不等待服务器响应，默认 false
	NoWait *bool
	// Args 额外参数，默认 nil
	Args amqp.Table
}

// getQueueConfig 获取队列配置，如果为 nil 则返回默认值
func getQueueConfig(cfg *QueueConfig) (durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
	if cfg == nil {
		// 默认值
		return true, false, false, false, nil
	}

	durable = true
	if cfg.Durable != nil {
		durable = *cfg.Durable
	}

	autoDelete = false
	if cfg.AutoDelete != nil {
		autoDelete = *cfg.AutoDelete
	}

	exclusive = false
	if cfg.Exclusive != nil {
		exclusive = *cfg.Exclusive
	}

	noWait = false
	if cfg.NoWait != nil {
		noWait = *cfg.NoWait
	}

	args = cfg.Args
	return
}

// BroadcastPublisher 实现广播发布者，使用 fanout 类型的交换机
type BroadcastPublisher struct {
	ch       *amqp.Channel
	exchange string
}

// NewBroadcastPublisher 创建一个广播发布者
func NewBroadcastPublisher(conn *amqp.Connection, exchange string) (*BroadcastPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}

	if err = declareBroadcastExchange(ch, exchange); err != nil {
		return nil, fmt.Errorf("cannot declare exchange: %v", err)
	}

	return &BroadcastPublisher{
		ch:       ch,
		exchange: exchange,
	}, nil
}

// Publish 发布广播消息
// fanout 交换机忽略 routingKey，所以这里不需要路由键
func (p *BroadcastPublisher) Publish(_ context.Context, msg interface{}) error {
	body, err := sonic.Marshal(msg)
	if err != nil {
		return fmt.Errorf("cannot marshal: %v", err)
	}

	// fanout 交换机忽略 routingKey，可以传空字符串
	return p.ch.Publish(p.exchange, "", false, false, amqp.Publishing{
		Body:         body,
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent, // 持久化消息
	})
}

// BindQueue 绑定队列到广播交换机
// 注意：fanout 交换机忽略 routingKey，但为了保持接口一致性，仍然接受该参数
// queueConfig 为 nil 时使用默认值（持久化=true, 自动删除=false, 独占=false, 无等待=false, 额外参数=nil）
func (p *BroadcastPublisher) BindQueue(queueName string, routingKey string, queueConfig *QueueConfig) error {
	durable, autoDelete, exclusive, noWait, args := getQueueConfig(queueConfig)

	// 声明一个队列
	q, err := p.ch.QueueDeclare(
		queueName,  // 队列名称
		durable,    // 持久化
		autoDelete, // 自动删除
		exclusive,  // 独占
		noWait,     // 无等待
		args,       // 额外参数
	)

	if err != nil {
		return fmt.Errorf("cannot declare queue: %v", err)
	}

	// 绑定队列到交换机
	// fanout 交换机忽略 routingKey，但为了保持接口一致性，仍然传递
	err = p.ch.QueueBind(
		q.Name,     // 队列名称
		routingKey, // 路由键（fanout 类型会忽略）
		p.exchange, // 交换机名称
		false,
		nil,
	)
	return err
}

// declareBroadcastExchange 声明 fanout 类型的交换机
func declareBroadcastExchange(ch *amqp.Channel, exchange string) error {
	return ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil)
}
