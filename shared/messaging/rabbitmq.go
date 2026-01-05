package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"ride-sharing/shared/contracts"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TripExchange = "trip"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	Channel *amqp.Channel
}

func NewRabbitMQ(uri string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create channel: %v", err)
	}

	rmq := &RabbitMQ{
		conn:    conn,
		Channel: ch,
	}

	if err := rmq.setupExchangesAndQueues(); err != nil {
		// Clean up if setup fails
		rmq.Close()
		return nil, fmt.Errorf("failed to setup exchanges and queues: %v", err)
	}

	return rmq, nil
}

type MessageHandler func(context.Context, amqp.Delivery) error

func (r *RabbitMQ) ConsumeMessages(queueName string, handler MessageHandler) error {
	err := r.Channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {

		return fmt.Errorf("failed to set QoS: %v", err)

	}

	msgs, err := r.Channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)

	if err != nil {
		fmt.Errorf("failed to consume the message of the queue: %s, - %v", queueName, err)
	}

	ctx := context.Background()

	go func() {
		for msg := range msgs {
			log.Printf("Received a message: %s", msg.Body)

			if err := handler(ctx, msg); err != nil {
				log.Fatal("Failed to handle the message: %v", err)
				if nackErr := msg.Nack(false, false); nackErr != nil {
					log.Printf("ERROR: Failed to Nack message: %v", nackErr)
				}
				continue
			}

			// Only Ack if the handler succeeds
			if ackErr := msg.Ack(false); ackErr != nil {
				log.Printf("ERROR: Failed to Ack message: %v. Message body: %s", ackErr, msg.Body)
			}
		}

	}()

	return nil
}

func (r *RabbitMQ) PublishMessage(ctx context.Context, routingKey string, message contracts.AmqpMessage) error {
	log.Printf("Publishing message with routing key: %s", routingKey)

	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("Failed to marshal message: %v", err)
	}

	return r.Channel.PublishWithContext(ctx,
		TripExchange, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         jsonMsg,
			DeliveryMode: amqp.Persistent,
		})
}

func (r *RabbitMQ) setupExchangesAndQueues() error {
	err := r.Channel.ExchangeDeclare(
		TripExchange,
		"topic",
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("Failed to declare an exchange: %s, : %v", TripExchange, err)
	}

	if err := r.declareAndBindQueue(FindAvailableDriversQueue, []string{contracts.TripEventCreated, contracts.TripEventDriverNotInterested}, TripExchange); err != nil {
		return fmt.Errorf("Failed to declare and Bind a Queue: %s, : %v", TripExchange, err)
	}

	return nil
}

func (r *RabbitMQ) declareAndBindQueue(queueName string, messageTypes []string, exchange string) error {
	q, err := r.Channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, msg := range messageTypes {
		if err := r.Channel.QueueBind(
			q.Name,   // queue name
			msg,      // routing key
			exchange, // exchange
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind the queue to %s; %v", queueName, err)
		}
	}
	return nil
}

func (r *RabbitMQ) Close() {
	if r.conn != nil {
		r.conn.Close()
	}
	if r.Channel != nil {
		r.Channel.Close()
	}
}
