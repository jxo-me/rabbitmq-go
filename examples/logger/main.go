package main

import (
	"context"
	"log"

	"github.com/jxo-me/rabbitmq-go"
)

// errorLogger is used in WithPublisherOptionsLogger to create a custom logger
// that only logs ERROR and FATAL log levels
type errorLogger struct{}

func (l errorLogger) Fatalf(ctx context.Context, format string, v ...interface{}) {
	log.Printf("mylogger: "+format, v...)
}

func (l errorLogger) Errorf(ctx context.Context, format string, v ...interface{}) {
	log.Printf("mylogger: "+format, v...)
}

func (l errorLogger) Warningf(ctx context.Context, format string, v ...interface{}) {
}

func (l errorLogger) Infof(ctx context.Context, format string, v ...interface{}) {
}

func (l errorLogger) Debugf(ctx context.Context, format string, v ...interface{}) {
}

func (l errorLogger) Noticef(ctx context.Context, format string, v ...interface{}) {}

func main() {
	ctx := context.Background()
	mylogger := &errorLogger{}

	publisher, err := rabbitmq.NewPublisher(
		ctx,
		"amqp://guest:guest@localhost", rabbitmq.Config{},
		rabbitmq.WithPublisherOptionsLogger(mylogger),
	)
	if err != nil {
		log.Fatal(err)
	}
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
		log.Fatal(err)
	}

	returns := publisher.NotifyReturn()
	go func() {
		for r := range returns {
			log.Printf("message returned from server: %s", string(r.Body))
		}
	}()
}
