package mq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	ch       *amqp.Channel
	exchange string
}

func NewProducer(ch *amqp.Channel, exchange string) *Producer {
	return &Producer{ch: ch, exchange: exchange}
}

func (p *Producer) Publish(ctx context.Context, routingKey string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return p.ch.PublishWithContext(ctx, p.exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (p *Producer) Close() {
	if p.ch != nil {
		p.ch.Close()
	}
	log.Print("mq producer closed")
}
