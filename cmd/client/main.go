package main

import (
	"fmt"

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
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Welcome to the Peril client, ", userName)
	queueName := "pause." + userName
	routingKey := routing.PauseKey
	boundChannel, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routingKey, 1)
	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}
	defer boundChannel.Close()
	gameState := gamelogic.NewGameState(userName)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routingKey,
		1,
		handlerPause(gameState),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to queue:", err)
		return
	}

	for {
		userInput := gamelogic.GetInput()
		if userInput[0] == "move" {
			gameState.CommandMove(userInput)
		} else if userInput[0] == "spawn" {
			gameState.CommandSpawn(userInput)
		} else if userInput[0] == "status" {
			gameState.CommandStatus()
		} else if userInput[0] == "help" {
			gamelogic.PrintClientHelp()
		} else if userInput[0] == "spam" {
			fmt.Println("Spamming not allowed yet!")
		} else if userInput[0] == "quit" {
			gamelogic.PrintQuit()
			break
		} else {
			fmt.Println("Invalid command. Type 'help' for a list of commands.")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}