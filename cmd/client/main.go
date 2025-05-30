package main

import (
	"fmt"
	"time"

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
	gameState := gamelogic.NewGameState(userName)
	defer boundChannel.Close()
	armyMovesChannel, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, userName, routing.ArmyMovesPrefix+".*", pubsub.Transient)
	warChan, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, userName, routing.WarRecognitionsPrefix+".*", pubsub.Transient)

	logChan, _, derr := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable)
	if derr != nil {
		fmt.Println("Failed to subscribe to queue:", derr)
		return
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, userName+"_"+routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+"."+userName, pubsub.Durable, handlerWar(gameState, logChan))
	if err != nil {
		fmt.Println("Failed to subscribe to queue:", err)
		return
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, userName, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gameState, warChan))
	if err != nil {
		fmt.Println("Failed to subscribe to queue:", err)
		return
	}
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
			move, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			fmt.Printf("publishing move: %s\n", routing.ArmyMovesPrefix+"."+userName)
			err = pubsub.PublishJSON(armyMovesChannel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+".*", move)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
				break
			}
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		if outcome == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		} else if outcome == gamelogic.MoveOutcomeMakeWar {
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+move.Player.Username, gamelogic.RecognitionOfWar{Attacker: move.Player, Defender: gs.Player})
			if err != nil {
				fmt.Println("Failed to publish message:", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		} else if outcome == gamelogic.MoveOutcomeSamePlayer {
			return pubsub.NackDiscard
		}
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		var msg string
		if outcome == gamelogic.WarOutcomeDraw {
			msg = "A war between " + winner + " and " + loser + " resulted in a draw"
		} else {
			msg = winner + " won a war against " + loser
		}

		fmt.Println(routing.ExchangePerilTopic, routing.GameLogSlug+"."+gs.Player.Username, outcome, msg)
		err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gs.Player.Username, routing.GameLog{
			CurrentTime: time.Now(),
			Message:     msg,
			Username:    gs.Player.Username,
		})
		if err != nil {
			fmt.Println("Error:", err)
			return pubsub.NackRequeue
		}
		if outcome == gamelogic.WarOutcomeNotInvolved {
			return pubsub.NackDiscard
		} else if outcome == gamelogic.WarOutcomeNoUnits {
			return pubsub.NackDiscard
		} else if outcome == gamelogic.WarOutcomeOpponentWon {
			return pubsub.Ack
		} else if outcome == gamelogic.WarOutcomeYouWon {
			return pubsub.Ack
		} else if outcome == gamelogic.WarOutcomeDraw {
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}
