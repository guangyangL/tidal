package mq

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageHandler func(body []byte) error

type Consumer struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	queue   string
	handler MessageHandler
}

func NewConsumer(url, exchange, queue, routingKey string, handler MessageHandler) (*Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("channel: %w", err)
	}
	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("exchange declare: %w", err)
	}
	q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("queue declare: %w", err)
	}
	if err := ch.QueueBind(q.Name, routingKey, exchange, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("queue bind: %w", err)
	}
	return &Consumer{conn: conn, ch: ch, queue: q.Name, handler: handler}, nil
}

func (c *Consumer) Start() error {
	msgs, err := c.ch.Consume(
		c.queue, "", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	go func() {
		for msg := range msgs {
			if err := c.handler(msg.Body); err != nil {
				log.Printf("consumer handle: %v, nack", err)
				msg.Nack(false, true) // requeue
			} else {
				msg.Ack(false)
			}
		}
	}()

	log.Print("mq consumer started")
	return nil
}

func (c *Consumer) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
	log.Print("mq consumer closed")
}
