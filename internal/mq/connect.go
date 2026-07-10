package mq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Connect(url, exchange string) (*amqp.Connection, func(), error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("exchange declare: %w", err)
	}

	return conn, func() { conn.Close() }, nil
}
