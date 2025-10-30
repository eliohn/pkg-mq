package amqpclt

import (
	"context"
	"fmt"
	"reflect"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

type DelayedSubscriber struct {
	Subscriber
}

func NewDelayedSubscriber(conn *amqp.Connection, exchange string, messageType reflect.Type) (*DelayedSubscriber, error) {
	// 与普通订阅者不同：这里显式声明 x-delayed-message 交换机
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}
	if err := declareDelayedExchange(ch, exchange); err != nil {
		return nil, fmt.Errorf("cannot declare delayed exchange: %v", err)
	}
	base := &Subscriber{
		ch:       ch,
		conn:     conn,
		exchange: exchange,
		dataType: messageType,
	}
	return &DelayedSubscriber{Subscriber: *base}, nil
}

func (s *DelayedSubscriber) BindQueue(queueName string, bindKey string) error {
	return s.Subscriber.BindQueue(queueName, bindKey)
}

func (s *DelayedSubscriber) Subscribe(ctx context.Context) (ch chan interface{}, err error) {
	msgCh, err := s.Subscriber.SubscribeRaw(ctx)
	if err != nil {
		//klog.Errorf("cannot subscribe: %s", err.Error())
		return nil, err
	}
	//klog.Info("subscribed")
	carCh := make(chan interface{})
	go func() {
		for msg := range msgCh {
			//klog.Infof("received message: %s", msg.Body)
			value := reflect.New(s.Subscriber.dataType).Interface()
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
