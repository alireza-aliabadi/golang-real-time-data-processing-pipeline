package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	rabbitmq "github.com/alireza-aliabadi/golang-real-time-data-processing-pipeline/internal"
)

func main() {
	conn, ch := rabbitmq.ConnectToRabbitmq()
	defer conn.Close()
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		"logs.dead-letter-q",
		true,
		false,
		false,
		false,
		nil,
	)
	rabbitmq.FailOnError(err, "Failed to declare 'logs.dead-letter' queue")

	messages, err := ch.Consume(
		queue.Name,
		"",
		true,
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
					log.Println(" [!] Shutdown signal received. Stopping consumer...")
					return
				case d, ok := <-messages:
					if !ok {
						log.Println(" [!] Channel closed by RabbitMQ")
						return
					}
					log.Printf("--- DEAD LETTER RECEIVED ---")
					log.Printf("Body: %s", d.Body)
					log.Printf("----------------------------")
			}
		}
	}()
	log.Printf(" [*] Inspector waiting for dead-letter logs. To exit press CTRL+C")
	wg.Wait()
	log.Println(" [x] Main process exited cleanly")
}