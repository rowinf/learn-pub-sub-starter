package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType int, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open a channel: %w", err)
	}
	queue, err := channel.QueueDeclare(
		queueName,
		simpleQueueType == 2, // durable
		simpleQueueType == 1, // delete when unused
		simpleQueueType == 1, // exclusive
		false,                // no-wait
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue: %w", err)
	}
	err = channel.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue: %w", err)
	}
	return channel, queue, nil
}

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType int, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	var err error
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind queue: %w", err)
	}
	msgs, err := channel.Consume(
		queue.Name,
		"",    // consumer tag
		false, // auto-acknowledge
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %w", err)
	}
	go func() {
		for msg := range msgs {
			var val T
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				fmt.Printf("failed to unmarshal message: %v\n", err)
				continue
			}
			ackType := handler(val)
			if ackType == Ack {
				err = msg.Ack(false) // acknowledge the message
				if err != nil {
					fmt.Printf("failed to acknowledge message: %v\n", err)
				}
			} else if ackType == NackRequeue {
				err = msg.Nack(false, true)
				if err != nil {
					fmt.Printf("failed to negatively acknowledge message (requeue): %v\n", err)
				}
			} else if ackType == NackDiscard {
				err = msg.Nack(false, false)
				if err != nil {
					fmt.Printf("failed to negatively acknowledge message (discard): %v\n", err)
				}
			}
		}
	}()

	return err
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var body bytes.Buffer
	enc := gob.NewEncoder(&body)
	err := enc.Encode(val)

	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        body.Bytes(),
	})
	if err != nil {
		fmt.Printf("Failed to publish game log: %v\n", err)
	} else {
		fmt.Println("Successfully published game log!")
	}
	return nil
}
