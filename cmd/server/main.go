package main

import (
	"encoding/json"
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
	_, _, derr := pubsub.DeclareAndBind(conn, exchange, queueName, routingKey, 2)
	if derr != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}

	for {
		userInput := gamelogic.GetInput()
		if userInput[0] == "pause" {
			fmt.Println("Sending a pause message...")
			exchange := routing.ExchangePerilDirect
			routingKey := routing.PauseKey
			message := routing.PlayingState{IsPaused: true}
			pubMsg, err := json.Marshal(message)
			err = pubsub.PublishJSON(pubChan, exchange, routingKey, pubMsg)

			if err != nil {
				fmt.Println("Failed to publish message:", err)
				break
			}
		} else if userInput[0] == "resume" {
			fmt.Println("Sending a resume message...")
			exchange := routing.ExchangePerilDirect
			routingKey := routing.PauseKey
			message := routing.PlayingState{IsPaused: false}
			pubMsg, err := json.Marshal(message)
			err = pubsub.PublishJSON(pubChan, exchange, routingKey, pubMsg)

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
