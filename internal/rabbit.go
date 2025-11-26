package internal

import (
	"log"
	amqp "github.com/rabbitmq/amqp091-go"
	config "github.com/alireza-aliabadi/golang-real-time-data-processing-pipeline/config"
)

// Helper to panic  on fatal errors
func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

//TODO: implement rabbitmq connection
func ConnectToRabbitmq() (*amqp.Connection, *amqp.Channel) {
	rabbitUrl := config.LoadConfig().RabbitmqUrl
	conn, err := amqp.Dial(rabbitUrl)
	FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")

	return conn, ch
}