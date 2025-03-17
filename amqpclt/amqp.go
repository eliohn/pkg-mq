package amqpclt

import (
	"context"
	"fmt"
	"reflect"

	"github.com/bytedance/sonic"
	"github.com/streadway/amqp"
)

// Publisher implements an amqp publisher.
type Publisher struct {
	ch         *amqp.Channel
	exchange   string
	routingKey string
}

func NewPublisher(conn *amqp.Connection, exchange string) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}

	if err = declareExchange(ch, exchange); err != nil {
		return nil, fmt.Errorf("cannot declare exchange: %v", err)
	}
	return &Publisher{
		ch:         ch,
		exchange:   exchange,
		routingKey: "",
	}, nil
}

// Publish publishes a message.
func (p *Publisher) Publish(_ context.Context, car interface{}) error {
	body, err := sonic.Marshal(car)
	if err != nil {
		return fmt.Errorf("cannot marshal: %v", err)
	}
	//klog.Infof("publishing message: %s", body)
	return p.ch.Publish(p.exchange, p.routingKey, false, false, amqp.Publishing{
		Body: body,
	})
}

func (p *Publisher) BindQueue(name string, bindKey string) error {
	p.routingKey = bindKey
	// 声明一个队列
	q, err := p.ch.QueueDeclare(
		name,  // 队列名称
		true,  // 持久化
		false, // 自动删除
		false, // 独占
		false, // 无等待
		nil,   // 额外参数
	)

	if err != nil {
		return fmt.Errorf("cannot declare queue: %v", err)
	}

	// 绑定队列到交换机
	err = p.ch.QueueBind(
		q.Name,     // 队列名称
		bindKey,    // 路由键
		p.exchange, // 交换机名称
		false,
		nil,
	)
	return err
}

// Subscriber implements an amqp subscriber.
type Subscriber struct {
	ch         *amqp.Channel
	conn       *amqp.Connection
	exchange   string
	queue      string
	routingKey string
	dataType   reflect.Type
}

// NewSubscriber creates an amqp subscriber.
func NewSubscriber(conn *amqp.Connection, exchange string, messageType reflect.Type) (*Subscriber, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}

	if err = declareExchange(ch, exchange); err != nil {
		return nil, fmt.Errorf("cannot declare exchange: %v", err)
	}

	return &Subscriber{
		ch:       ch,
		conn:     conn,
		exchange: exchange,
		dataType: messageType,
	}, nil
}

func (s *Subscriber) BindQueue(queueName string, bindKey string) error {
	s.routingKey = bindKey
	s.queue = queueName
	// 声明一个队列
	q, err := s.ch.QueueDeclare(
		queueName, // 队列名称
		true,      // 持久化
		false,     // 自动删除
		false,     // 独占
		false,     // 无等待
		nil,       // 额外参数
	)

	if err != nil {
		return fmt.Errorf("cannot declare queue: %v", err)
	}

	// 绑定队列到交换机
	err = s.ch.QueueBind(
		q.Name,     // 队列名称
		bindKey,    // 路由键
		s.exchange, // 交换机名称
		false,
		nil,
	)
	return err
}

// SubscribeRaw subscribes and returns a channel with raw amqp delivery.
func (s *Subscriber) SubscribeRaw(_ context.Context) (<-chan amqp.Delivery, error) {

	msgs, err := s.ch.Consume(s.queue, "", true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot consume queue: %v", err)
	}
	return msgs, nil
}

// Subscribe subscribes and returns a channel with CarEntity data.
func (s *Subscriber) Subscribe(c context.Context) (chan interface{}, error) {
	msgCh, err := s.SubscribeRaw(c)
	if err != nil {
		return nil, err
	}

	carCh := make(chan interface{})
	go func() {
		for msg := range msgCh {
			value := reflect.New(s.dataType).Interface()
			err := sonic.Unmarshal(msg.Body, value)
			if err != nil {
				//klog.Errorf("cannot unmarshal %s", err.Error())
			}
			carCh <- value
		}
		close(carCh)
	}()
	return carCh, nil
}

func declareExchange(ch *amqp.Channel, exchange string) error {
	return ch.ExchangeDeclare(exchange, "x-delayed-message", true, false, false, false, amqp.Table{
		"x-delayed-type": "direct", // 这里指定底层交换机类型
	})
}
