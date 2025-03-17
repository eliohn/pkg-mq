package mq

import (
	"context"
)

// Publisher defines the publish interface.
type Publisher interface {
	Publish(context.Context, interface{}) error
	BindQueue(string, string, string) error
}

// Subscriber defines a car update subscriber.
type Subscriber interface {
	Subscribe(context.Context) (ch chan interface{}, err error)
}
