package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jxo-me/rabbitmq-go"
)

func main() {
	ctx := context.Background()
	conn, err := rabbitmq.NewConn(
		ctx,
		"amqp://guest:guest@localhost",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	consumer, err := rabbitmq.NewConsumer(
		ctx,
		conn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithConsumerOptionsConcurrency(2),
		rabbitmq.WithConsumerOptionsConsumerName("consumer_1"),
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key_2"),
		rabbitmq.WithConsumerOptionsExchangeName("events"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close(ctx)

	consumer2, err := rabbitmq.NewConsumer(
		ctx,
		conn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			log.Printf("consumed 2: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithConsumerOptionsConcurrency(2),
		rabbitmq.WithConsumerOptionsConsumerName("consumer_2"),
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
		rabbitmq.WithConsumerOptionsExchangeName("events"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer2.Close(ctx)

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
	<-done
	fmt.Println("stopping consumer")
}
