package main

import (
	"bufio"
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

func sendToRabbit(connectionStr string, messages chan []byte, exchange string) {
	conn, err := amqp.Dial(connectionStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	for msg := range messages {
		fmt.Fprintf(os.Stderr, "Trying to send %s... ", msg[:10])
		err = ch.Publish(
			exchange,
			"",    // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        msg,
			})
		failOnError(err, "Failed to publish a message")
		fmt.Fprintf(os.Stderr, " DONE\n")
	}
}

func listenToRabbit(connectionStr string, queue_name string, messages chan []byte) {
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

	fromQueuePtr := flag.String("q", "", "The queue from wich to take the messages out of. OR - to read from stdin.")
	toExchangePtr := flag.String("x", "", "The exchange all the messages should be passed on to.")
	connectionStrPtr := flag.String("rabbit", "amqp://guest:guest@localhost:5672/", "Rabbmit MQ connection string.")

	flag.Parse()

	fromStdIn := false
	from := *fromQueuePtr
	if *fromQueuePtr == "-" {
		fromStdIn = true
		from = "stdin"
	}

	fmt.Fprintf(os.Stderr, "Host: %s\n", *connectionStrPtr)
	fmt.Fprintf(os.Stderr, "%s --->> %s\n", from, *toExchangePtr)

	messages := make(chan []byte)

	go sendToRabbit(*connectionStrPtr, messages, *toExchangePtr)

	if fromStdIn {
		// processing file
		fmt.Fprintf(os.Stderr, "Reading input from Stdin.\n")
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			messages <- []byte(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			failOnError(err, "Failed reading Stdin.")
		} else {
			fmt.Fprintf(os.Stderr, "Done reading Stdin.\n")
		}
	} else {
		// consuming rabbit
		fmt.Fprintf(os.Stderr, "Listening for messages from RabbitMQ ...\n")
		go listenToRabbit(*connectionStrPtr, *fromQueuePtr, messages)
	}
	forever := make(chan bool)
	fmt.Fprintln(os.Stderr, "Send Ctrl-C to stop...")
	<-forever
}
