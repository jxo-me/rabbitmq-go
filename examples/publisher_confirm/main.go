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
	conn, err := rabbitmq.NewConn(
		ctx,
		"amqp://guest:guest@localhost",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	publisher, err := rabbitmq.NewPublisher(
		ctx,
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName("events"),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsConfirm,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close(ctx)

	publisher.NotifyReturn(func(r rabbitmq.Return) {
		log.Printf("message returned from server: %s", string(r.Body))
	})

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
			confirms, err := publisher.PublishWithDeferredConfirmWithContext(
				context.Background(),
				[]byte("hello, world"),
				[]string{"my_routing_key"},
				rabbitmq.WithPublishOptionsContentType("application/json"),
				rabbitmq.WithPublishOptionsMandatory,
				rabbitmq.WithPublishOptionsPersistentDelivery,
				rabbitmq.WithPublishOptionsExchange("events"),
			)
			if err != nil {
				log.Println(err)
				continue
			} else if len(confirms) == 0 || confirms[0] == nil {
				fmt.Println("message publishing not confirmed")
				continue
			}
			fmt.Println("message published")
			ok, err := confirms[0].WaitContext(context.Background())
			if err != nil {
				log.Println(err)
			}
			if ok {
				fmt.Println("message publishing confirmed")
			} else {
				fmt.Println("message publishing not confirmed")
			}
		case <-done:
			fmt.Println("stopping publisher")
			return
		}
	}
}
