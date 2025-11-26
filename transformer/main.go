package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/alireza-aliabadi/golang-real-time-data-processing-pipeline/internal"
)

func main() {
	conn, ch := rabbitmq.ConnectToRabbitmq()
	defer conn.Close()
	defer ch.Close()

	err := ch.ExchangeDeclare(
		"logs.data",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.data' exchange")

	queue, err := ch.QueueDeclare(
		"logs.to-transform",
		true,
		false,
		false,
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.to-transform' queue")

	err = ch.QueueBind(queue.Name, "logs.valid", "logs.data", false, nil)
	rabbitmq.FailOnError(err, "Failed to bind queue")

	err = ch.Qos(1, 0, false)
	rabbitmq.FailOnError(err, "Failed to set QoS")

	messages, err := ch.Consume(
		queue.Name,
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

	go func()  {
		defer wg.Done()

		for {
			select {
				case <-ctx.Done():
					log.Println(" [!] Shutdown signal received. Stopping transformer...")
					return
				case d, ok := <-messages:
					if !ok {
						log.Println(" [!] RabbitMQ channel closed. Exiting.")
						return
					}
					var logMsg rabbitmq.LogMessage
					err := json.Unmarshal(d.Body, &logMsg)
					if err != nil {
						log.Printf(" [!] Fatal Unmarshal Error (Validator failed?): %s. Discarding.", err)
						d.Nack(false, false)
						continue
					}

					logMsg.ProcessedAt = time.Now().Format(time.RFC3339)
					body, err := json.Marshal(logMsg)
					if err != nil {
						log.Printf(" [!] FATAL Marshal Error (Code Bug): %s. Requeuing.", err)
						d.Nack(false, true)
						continue
					}

					pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
					err = ch.PublishWithContext(pubCtx,
						"logs.data",
						"logs.processed",
						false,
						false,
						amqp.Publishing{
							ContentType: "application/json",
							Body: body,
							DeliveryMode: amqp.Persistent,
							MessageId: logMsg.ID,
						},
					)
					pubCancel()
					if err != nil {
						log.Printf(" [!] Error publishing processed log: %s. Requeuing.", err)
						d.Nack(false, true)
						continue
					}
					d.Ack(false)
					log.Printf(" [x] Transformed log: %s", logMsg.ID)
			}
		}
	}()
	log.Printf(" [*] Transformer waiting for valid logs. To exit press CTRL+C")
	wg.Wait()
	log.Println(" [x] Main process exited cleanly")
}