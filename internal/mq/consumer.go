package mq

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageHandler func(body []byte) error

type Consumer struct {
	ch      *amqp.Channel
	queue   string
	handler MessageHandler
}

func NewConsumer(ch *amqp.Channel, exchange, queue, routingKey string, handler MessageHandler) (*Consumer, error) {
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("queue declare: %w", err)
	}
	if err := ch.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		return nil, fmt.Errorf("queue bind: %w", err)
	}
	return &Consumer{ch: ch, queue: queue, handler: handler}, nil
}

func (c *Consumer) Start() error {
	msgs, err := c.ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	go func() {
		for msg := range msgs {
			if err := c.handler(msg.Body); err != nil {
				log.Printf("consumer handle: %v, nack", err)
				msg.Nack(false, true)
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
	log.Print("mq consumer closed")
}
