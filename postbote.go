package main

import (
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"os"
)

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s", msg, err)
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

func listen(connectionStr string, queue_name string, messages chan []byte) {
	conn, err := amqp.Dial(connectionStr)
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
		fmt.Fprintf(os.Stderr, "message received!\n")
		fmt.Println(string(message[:]))
		messages <- message
	}
}

func main() {

	fromQueuePtr := flag.String("q", "", "The queue from wich to take the messages out of.")
	toExchangePtr := flag.String("x", "", "The exchange all the messages should be passed on to.")

	connectionStrPtr := flag.String("rabbit", "amqp://guest:guest@localhost:5672/", "Rabbmit MQ connection string.")

	flag.Parse()

	fmt.Fprintf(os.Stderr, "Host: %s\n", *connectionStrPtr)
	fmt.Fprintf(os.Stderr, "%s --->> %s\n", *fromQueuePtr, *toExchangePtr)

	messages := make(chan []byte)
	go listen(*connectionStrPtr, *fromQueuePtr, messages)
	go send(messages, *toExchangePtr)

	forever := make(chan bool)
	fmt.Fprintln(os.Stderr, "Listening for messages! Send Ctrl-C to stop...")
	<-forever
}
