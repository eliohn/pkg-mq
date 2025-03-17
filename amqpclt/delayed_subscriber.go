package amqpclt

import (
	"context"
	"reflect"

	"github.com/bytedance/sonic"
	"github.com/streadway/amqp"
)

type DelayedSubscriber struct {
	Subscriber
}

func NewDelayedSubscriber(conn *amqp.Connection, exchange string, messageType reflect.Type) (*DelayedSubscriber, error) {
	subscriber, err := NewSubscriber(conn, exchange, messageType)
	if err != nil {
		return nil, err
	}
	return &DelayedSubscriber{
		Subscriber: *subscriber,
	}, nil
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
