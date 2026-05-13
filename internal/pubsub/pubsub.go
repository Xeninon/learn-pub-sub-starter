package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: data})
	if err != nil {
		return err
	}

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var payload T
			if err := json.Unmarshal(delivery.Body, &payload); err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
			}

			switch handler(payload) {
			case Ack:
				if err = delivery.Ack(false); err != nil {
					fmt.Printf("could not ack message: %v\n", err)
				}
				log.Println("Ack")
			case NackRequeue:
				if err = delivery.Nack(false, true); err != nil {
					fmt.Printf("could not Nack message: %v\n", err)
				}
				log.Println("NackR")
			case NackDiscard:
				if err = delivery.Nack(false, false); err != nil {
					fmt.Printf("could not Nack message: %v\n", err)
				}
				log.Println("NackD")
			}

		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	properties := amqp.NewConnectionProperties()
	properties["x-dead-letter-exchange"] = "peril_dlx"
	queue, err := channel.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, properties)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channel.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return channel, queue, nil
}
