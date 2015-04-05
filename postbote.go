package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func send(messages chan []byte, exchange string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	for {
		err = ch.Publish(
			exchange,
			"",    // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        <-messages,
			})
		failOnError(err, "Failed to publish a message")
	}
}

func listen(queue_name string, messages chan []byte) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		queue_name, // queue
		"postbote", // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	for rabbmit_mq_payload := range msgs {
		message := rabbmit_mq_payload.Body
		fmt.Printf("received message: %s", string(message[:]))
		messages <- message
	}
}

func main() {
	messages := make(chan []byte)
	go listen("inbox.errorsQ", messages)
	go send(messages, "inbox")

	forever := make(chan bool)
	fmt.Printf("Listenting for messages. Send Ctrl-C to stop...")
	<-forever
}
