package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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
	gamelogic.PrintServerHelp()
	pubChan, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}
	defer pubChan.Close()
	exchange := routing.ExchangePerilTopic
	queueName := routing.GameLogSlug
	routingKey := routing.GameLogSlug + ".*"
	derr := pubsub.SubscribeGOB(conn, exchange, queueName, routingKey, pubsub.Durable, handlerGameLog)
	if derr != nil {
		fmt.Println("Failed to declare and bind queue:", derr)
		return
	}

	for {
		userInput := gamelogic.GetInput()
		if userInput[0] == "pause" {
			fmt.Println("Sending a pause message...")
			exchange := routing.ExchangePerilDirect
			routingKey := routing.PauseKey
			message := routing.PlayingState{IsPaused: true}
			err = pubsub.PublishJSON(pubChan, exchange, routingKey, message)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
				break
			}
		} else if userInput[0] == "resume" {
			fmt.Println("Sending a resume message...")
			exchange := routing.ExchangePerilDirect
			routingKey := routing.PauseKey
			message := routing.PlayingState{IsPaused: false}
			err = pubsub.PublishJSON(pubChan, exchange, routingKey, message)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
				break
			}
		} else if userInput[0] == "quit" {
			fmt.Println("Quitting...")
			break
		} else {
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}
}

func handlerGameLog(gl routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	gamelogic.WriteLog(gl)
	return pubsub.Ack
}
