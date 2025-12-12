package amqpclt

import (
	"context"
	"fmt"
	"reflect"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

// BroadcastSubscriber 实现广播订阅者，使用 fanout 类型的交换机
type BroadcastSubscriber struct {
	ch         *amqp.Channel
	conn       *amqp.Connection
	exchange   string
	queue      string
	routingKey string
	dataType   reflect.Type
}

// NewBroadcastSubscriber 创建一个广播订阅者
func NewBroadcastSubscriber(conn *amqp.Connection, exchange string, messageType reflect.Type) (*BroadcastSubscriber, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}

	if err = declareBroadcastExchange(ch, exchange); err != nil {
		return nil, fmt.Errorf("cannot declare exchange: %v", err)
	}

	return &BroadcastSubscriber{
		ch:       ch,
		conn:     conn,
		exchange: exchange,
		dataType: messageType,
	}, nil
}

// BindQueue 绑定队列到广播交换机
// 注意：fanout 交换机忽略 routingKey，但为了保持接口一致性，仍然接受该参数
// queueConfig 为 nil 时使用默认值（持久化=true, 自动删除=false, 独占=false, 无等待=false, 额外参数=nil）
func (s *BroadcastSubscriber) BindQueue(queueName string, routingKey string, queueConfig *QueueConfig) error {
	s.routingKey = routingKey
	s.queue = queueName

	durable, autoDelete, exclusive, noWait, args := getQueueConfig(queueConfig)
	
	// 声明一个队列
	q, err := s.ch.QueueDeclare(
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
	err = s.ch.QueueBind(
		q.Name,     // 队列名称
		routingKey, // 路由键（fanout 类型会忽略）
		s.exchange, // 交换机名称
		false,
		nil,
	)
	return err
}

// SubscribeRaw 订阅并返回原始 amqp delivery 通道
func (s *BroadcastSubscriber) SubscribeRaw(_ context.Context) (<-chan amqp.Delivery, error) {
	msgs, err := s.ch.Consume(s.queue, "", true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot consume queue: %v", err)
	}
	return msgs, nil
}

// Subscribe 订阅并返回反序列化后的消息通道
func (s *BroadcastSubscriber) Subscribe(c context.Context) (chan interface{}, error) {
	msgCh, err := s.SubscribeRaw(c)
	if err != nil {
		return nil, err
	}

	msgTypeCh := make(chan interface{})
	go func() {
		for msg := range msgCh {
			value := reflect.New(s.dataType).Interface()
			err := sonic.Unmarshal(msg.Body, value)
			if err != nil {
				// 反序列化失败，跳过该消息
				continue
			}
			msgTypeCh <- value
		}
		close(msgTypeCh)
	}()
	return msgTypeCh, nil
}

