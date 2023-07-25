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
		rabbitmq.WithPublisherOptionsLogger(mylogger),
	)
	if err != nil {
		log.Fatal(err)
	}
	err = publisher.PublishWithContext(
		context.Background(),
		[]byte("hello, world"),
		[]string{"my_routing_key"},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange("events"),
	)
	if err != nil {
		log.Fatal(err)
	}

	publisher.NotifyReturn(func(r rabbitmq.Return) {
		log.Printf("message returned from server: %s", string(r.Body))
	})
}
