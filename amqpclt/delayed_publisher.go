package amqpclt

import (
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/streadway/amqp"
	"time"
)

type DelayedPublisher struct {
	ch         *amqp.Channel
	exchange   string
	delay      int64 //延时时间（ms）
	routingKey string
}

func NewDelayedPublisher(conn *amqp.Connection, exchange string, delay time.Duration) (*DelayedPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}

	if err = declareDelayedExchange(ch, exchange); err != nil {
		//klog.Fatal("cannot dial amqp", err)
		return nil, fmt.Errorf("cannot declare exchange: %v", err)
	}

	return &DelayedPublisher{
		ch:         ch,
		exchange:   exchange,
		delay:      delay.Milliseconds(),
		routingKey: "",
	}, nil
}
func declareDelayedExchange(ch *amqp.Channel, exchange string) error {
	return ch.ExchangeDeclare(exchange, "x-delayed-message", true, false, false, false,
		amqp.Table{
			"x-delayed-type": "direct",
		})

}

// Publish publishes a message.
func (p *DelayedPublisher) Publish(_ context.Context, car interface{}) error {
	body, err := sonic.Marshal(car)
	if err != nil {
		return fmt.Errorf("cannot marshal: %v", err)
	}
	//klog.Infof("publishing message: %s", body)
	return p.ch.Publish(p.exchange, p.routingKey, false, false, amqp.Publishing{
		Headers: amqp.Table{
			"x-delay": p.delay,
		},
		ContentType:  "text/plain",
		Body:         body,
		DeliveryMode: amqp.Persistent, // 持久化消息
	})
}

func (p *DelayedPublisher) BindQueue(queueName string, bindKey string) error {
	p.routingKey = bindKey
	// 声明一个队列
	q, err := p.ch.QueueDeclare(
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
	err = p.ch.QueueBind(
		q.Name,     // 队列名称
		bindKey,    // 路由键
		p.exchange, // 交换机名称
		false,
		nil,
	)

	return nil
}
