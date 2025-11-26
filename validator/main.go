package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/alireza-aliabadi/golang-real-time-data-processing-pipeline/internal"
)

func main() {
	connection, ch := rabbitmq.ConnectToRabbitmq()
	defer connection.Close()
	defer ch.Close()

	// Declare all exchanges
	err := ch.ExchangeDeclare("logs.ingest", "fanout", true, false, false, false, nil)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.ingest'")
	err = ch.ExchangeDeclare("logs.data", "direct", true, false, false, false, nil)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.data'")

	// Declare the dead letter exchange
	err = ch.ExchangeDeclare(
		"logs.dead-letter-x",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.dead-letter-x'")

	//Declare the dead letter queue
	dlq, err := ch.QueueDeclare(
		"logs.dead-letter-q",
		true,
		false,
		false,
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.dead-letter-q' queue")

	// Bind dead letter queue to dead letter exchange
	err = ch.QueueBind(
		dlq.Name,
		"logs.invalid",
		"logs.dead-letter-x",
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to bind dead letter queue")

	// Declare the main validation queue with dead-letter exchange arguments
	q, err := ch.QueueDeclare(
		"logs.to-validate",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "logs.dead-letter-x",
			"x-dead-letter-routing-key": "logs.invalid",
		},
	)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.to-validate' queue")

	// Bind the validation queue to the ingest exchange
	err = ch.QueueBind(
		q.Name,
		"",
		"logs.ingest",
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to bind queue to exchange")

	// Set QoS with optimization 3
	err = ch.Qos(1, 0, false)
	rabbitmq.FailOnError(err, "Failed to set QoS")

	messages, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println(" [*] Consumer loop started")
		
		for {
			select {
			// Case 1: We received a shutdown signal
			case <-ctx.Done():
				log.Println(" [!] Shutdown signal received. Stopping consumer...")
				return 
			
			// Case 2: We received a message from RabbitMQ
			case d, ok := <-messages:
				if !ok {
					log.Println(" [!] Channel closed by RabbitMQ")
					return
				}
				log.Printf(" [!] Message Received! evaluation...")
				var logMsg rabbitmq.LogMessage
				if err := json.Unmarshal(d.Body, &logMsg); err != nil {
					log.Printf(" [!] Malformed JSON, discarding. Error: %v", err)
					if err := d.Nack(false, false); err != nil {
						log.Printf(" [!] Failed to nack message: %v", err)
					}
					continue
				}
				if logMsg.ID == "" || logMsg.Service == "" || logMsg.Level == "" {
					log.Printf(" [!] Validation failed (missing ID or Service or Level). Discarding.")
					if err := d.Nack(false, false); err != nil {
						log.Printf(" [!] Failed to nack message: %v", err)
					}
					continue
				}
				body, err := json.Marshal(logMsg)
				if err != nil {
					log.Printf(" [!] Failed to re-marshal validated log: %v", err)
					if err := d.Nack(false, false); err != nil {
						log.Printf(" [!] Failed to nack message: %v", err)
					}
					continue
				}
				err = ch.PublishWithContext(ctx,
					"logs.data",
					"logs.valid",
					false,
					false,
					amqp.Publishing{
						ContentType: "application/json",
						Body: body,
						DeliveryMode: amqp.Persistent,
						MessageId: logMsg.ID,
					},
				)
				if err != nil {
					log.Printf(" [!] Failed to publish validated log: %v", err)
					if err := d.Nack(false, false); err != nil {
						log.Printf(" [!] Failed to nack message: %v", err)
					}
					continue
				}
				if err := d.Ack(false); err != nil {
					log.Printf(" [!] Failed to ack message: %v", err)
				}
				log.Printf(" [x] Validated and forwarded log: %s", logMsg.ID)
			}
		}
	}()
	log.Printf(" [*] Validator running. Press CTRL+C to exit.")
	
	// This blocks until the consumer goroutine returns (which happens on CTRL+C)
	wg.Wait() 
	log.Println(" [x] Main process exited cleanly")
}