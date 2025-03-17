package amqpclt

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMqQueue struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	// 队列名称
	QueueName string
	//交换机
	Exchange string
	//key
	Key string
}

// 创建结构体实例
func NewRabbitMQ(conn *amqp.Connection, queueName string, exchange string, key string) *RabbitMqQueue {
	rabbitmq := &RabbitMqQueue{QueueName: queueName, Exchange: exchange, Key: key}
	var err error
	// 创建rabbitmq连接
	rabbitmq.conn = conn
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取channel失败！")
	return rabbitmq
}

// 断开channel和connection
func (r *RabbitMqQueue) Destory() {
	r.channel.Close()

}

// 错误处理函数
func (r *RabbitMqQueue) failOnErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}

// 简单模式step1： 1.创建简单模式下的rabbitmq实例
func NewRabbitMQSimple(conn *amqp.Connection, queueName string) *RabbitMqQueue {
	return NewRabbitMQ(conn, queueName, "", "")
}

// 简单模式step2： 2.简单模式下生产
func (r *RabbitMqQueue) PublishSimple(message []byte) error {
	// 1. 申请队列，如果队列不存在则自动创建，如果存在则跳过创建
	// 保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		// 是否持久化
		true,
		// 是否为自动删除
		false,
		// 是否具有排他性
		false,
		// 是否阻塞
		false,
		// 额外属性
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return err
	}
	// 2. 发送消息到队列中
	err = r.channel.Publish(
		r.Exchange,
		r.QueueName,
		// 如果为true,根据exchange类型和routekey规则，如果无法找到符合条件的队列，则会把发送的消息返回给发送者
		false,
		// 如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者，则会把消息发还给发送者
		false,
		amqp.Publishing{ContentType: "text/plain", Body: message},
	)
	if err != nil {
		return err
	}
	return nil
}

// 简单模式step3： 3.简单模式下消费
func (r *RabbitMqQueue) ConsumeSimple() {
	// 1. 申请队列，如果队列不存在则自动创建，如果存在则跳过创建
	// 保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		// 是否持久化
		true,
		// 是否为自动删除
		false,
		// 是否具有排他性
		false,
		// 是否阻塞
		false,
		// 额外属性
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	// 2. 接收消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		// 用来区分多个消费者
		"",
		// 是否自动应答
		true,
		// 是否具有排他性
		false,
		// 如果为true，表示不能将同一个conn中的消息发送给这个conn中的消费者
		false,
		// 队列是否阻塞
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	forever := make(chan bool)
	// 3. 启用协程处理消息
	go func() {
		for d := range msgs {
			// 实现我们要处理的逻辑函数
			log.Printf("Received a message: %s", d.Body)
		}
	}()
	log.Printf("[*] waiting for messages, to exit process CTRL+C")
	<-forever
}
