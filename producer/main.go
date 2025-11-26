package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/alireza-aliabadi/golang-real-time-data-processing-pipeline/internal"
)

func publishLog(ctx context.Context, ch *amqp.Channel, message rabbitmq.LogMessage) {
	body, err := json.Marshal(message)
	rabbitmq.FailOnError(err, "Failed to marshal log")

	err = ch.PublishWithContext(ctx,
		"logs.ingest",
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: body,
			DeliveryMode: amqp.Persistent,
			MessageId: message.ID,
		})
	rabbitmq.FailOnError(err, "Failed to publish valid log")
	log.Printf(" [x] Sent valid log with ID: %s", message.ID)
}

func main() {
	conn, ch := rabbitmq.ConnectToRabbitmq()
	defer conn.Close()
	defer ch.Close()

	// Declare the Ingest exchange
	err := ch.ExchangeDeclare(
		"logs.ingest",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)

	rabbitmq.FailOnError(err, "Failed to declare 'logs.ingest' exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	validLog := rabbitmq.LogMessage{
		ID: uuid.NewString(),
		Timestamp: time.Now().Unix(),
		Level: "info",
		Service: "producer-service",
		Message: "This is a valid log message",
	}

	// This log is "invalid" because it's missing the 'Level' field
	invalidLog := `{"id": "` + uuid.NewString() + `", "service": "unknown", "message": "Missing level"}`

	// publishing valid log
	publishLog(ctx, ch, validLog)

	// publishing invalid log
	err = ch.PublishWithContext(ctx,
		"logs.ingest",
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: []byte(invalidLog),
			DeliveryMode: amqp.Persistent,
		})
	rabbitmq.FailOnError(err, "Failed to publish invalid log")
	log.Println(" [x] Sent invalid log")
}