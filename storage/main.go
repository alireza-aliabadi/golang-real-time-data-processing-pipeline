package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	rabbitmq "github.com/alireza-aliabadi/golang-real-time-data-processing-pipeline/internal"
)

// for develop, in production should be redis or db
var processedIDs = make(map[string]bool)

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
		"logs.to-store",
		true,
		false,
		false,
		false,
		nil,
	)

	err = ch.QueueBind(
		queue.Name,
		"logs.processed",
		"logs.data",
		false,
		nil,
	)
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

	var wg sync.WaitGroup
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	wg.Add(1)
	go func ()  {
		defer wg.Done()

		for {
			select {
				case <-ctx.Done():
					log.Println(" [!] Shutdown signal received. Stopping consumer...")
					return
				case d, ok := <-messages:
					if !ok {
						log.Println(" [!] Channel closed by RabbitMQ")
						return
					}
					var logMsg rabbitmq.LogMessage
					if err := json.Unmarshal(d.Body, &logMsg); err != nil {
						log.Printf(" [!] Malformed JSON, discarding: %s. Error: %v", d.Body, err)
						d.Nack(false, false) // Nack, do not requeue (bad data)
						continue
					} 
					if _, exists := processedIDs[logMsg.ID]; exists {
						log.Printf(" [!] Duplicate message received, ignoring: %s", logMsg.ID)
						d.Ack(false)
						continue
					}
					log.Printf("--- FINAL PROCESSED LOG ---")
					log.Printf("ID: %s, Service: %s, Msg: %s", logMsg.ID, logMsg.Service, logMsg.Message)
					log.Printf("--------------------------")

					processedIDs[logMsg.ID] = true
					d.Ack(false)
					log.Printf(" [x] Successfully stored and acknowledged log: %s", logMsg.ID)
			}
		}
	}()
	log.Printf(" [*] Storage service waiting for processed logs. To exit press CTRL+C")
	wg.Wait()
	log.Println(" [x] Main process exited cleanly")
}