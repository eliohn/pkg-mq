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
type RabbitMqConfig struct {
	Host        string `mapstructure:"host" json:"host" yaml:"host"`
	Port        int    `mapstructure:"port" json:"port" yaml:"port"`
	Exchange    string `mapstructure:"exchange" json:"exchange" yaml:"exchange"`
	Username    string `mapstructure:"username" json:"username" yaml:"username"`
	Password    string `mapstructure:"password" json:"password" yaml:"password"`
	VirtualHost string `mapstructure:"virtual_host" json:"virtual_host" yaml:"virtual_host"`
}
