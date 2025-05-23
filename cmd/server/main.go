package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connection)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")
	fmt.Println("Starting Peril server...")
	// Wait for Ctrl+C (SIGINT) to exit gracefully
	signalChan := make(chan os.Signal, 1)
	pubChan, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}
	defer pubChan.Close()

	// Publish a message to the "test" exchange with the routing key "test"
	// Replace with your exchange and routing key
	exchange := routing.ExchangePerilDirect
	routingKey := routing.PauseKey
	// Replace with your message
	message := routing.PlayingState{IsPaused: true}
	pubMsg, err := json.Marshal(message)
	// Replace with your message
	err = pubsub.PublishJSON(pubChan, exchange, routingKey, pubMsg)

	if err != nil {
		fmt.Println("Failed to publish message:", err)
		return
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		fmt.Println("\nReceived interrupt. Shutting down...")
		close(signalChan)
	}()

	<-signalChan
}
