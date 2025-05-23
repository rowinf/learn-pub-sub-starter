package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connection)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	// Wait for Ctrl+C (SIGINT) to exit gracefully
	signalChan := make(chan os.Signal, 1)
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")
	welcome, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Welcome to the Peril client, ", welcome)
	queueName := "pause.suntzu"
	routingKey := routing.PauseKey
	boundChannel, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routingKey, 1)
	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}
	defer boundChannel.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		fmt.Println("\nReceived interrupt. Shutting down...")
		close(signalChan)
	}()

	<-signalChan
}
