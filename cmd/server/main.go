package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/Peril/internal/gamelogic"
	"github.com/bootdotdev/Peril/internal/pubsub"
	"github.com/bootdotdev/Peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal("Error connecting to rabbitmq")
	}
	defer connection.Close()

	fmt.Println("Connection successful")

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error creating channel: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		quit := false
		switch input[0] {
		case "pause":
			fmt.Println("Sending pause message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Error publishing json: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Error publishing json: %v", err)
			}
		case "quit":
			fmt.Println("Exiting")
			quit = true
		default:
			fmt.Println("Unknown command")
		}
		if quit {
			break
		}
	}
}
