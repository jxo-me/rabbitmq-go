package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jxo-me/rabbitmq-go"
)

func main() {
	ctx := context.Background()
	publisher, err := rabbitmq.NewPublisher(
		ctx,
		"amqp://guest:guest@localhost", rabbitmq.Config{},
		rabbitmq.WithPublisherOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := publisher.Close(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	returns := publisher.NotifyReturn()
	go func() {
		for r := range returns {
			log.Printf("message returned from server: %s", string(r.Body))
		}
	}()

	confirmations := publisher.NotifyPublish(ctx)
	go func() {
		for c := range confirmations {
			log.Printf("message confirmed from server. tag: %v, ack: %v", c.DeliveryTag, c.Ack)
		}
	}()

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	fmt.Println("awaiting signal")

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			err = publisher.Publish(
				ctx,
				[]byte("hello, world"),
				[]string{"routing_key"},
				rabbitmq.WithPublishOptionsContentType("application/json"),
				rabbitmq.WithPublishOptionsMandatory,
				rabbitmq.WithPublishOptionsPersistentDelivery,
				rabbitmq.WithPublishOptionsExchange("events"),
			)
			if err != nil {
				log.Println(err)
			}
		case <-done:
			fmt.Println("stopping publisher")
			return
		}
	}
}
