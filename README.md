# rabbitmq-go

Wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐


## Motivation

[Streadway's AMQP](https://github.com/rabbitmq/amqp091-go) library is currently the most robust and well-supported Go client I'm aware of. It's a fantastic option and I recommend starting there and seeing if it fulfills your needs. Their project has made an effort to stay within the scope of the AMQP protocol, as such, no reconnection logic and few ease-of-use abstractions are provided.

### Goal

The goal with `rabbitmq-go` is to still provide most all of the nitty-gritty functionality of AMQP, but to make it easier to work with via a higher-level API. Particularly:

* Automatic reconnection
* Multithreaded consumers via a handler function
* Reasonable defaults
* Flow control handling
* TCP block handling

## ⚙️ Installation

Inside a Go module:

```bash
go get github.com/jxo-me/rabbitmq-go
```

## 🚀 Quick Start Consumer

### Default options

```go

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
        rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
        rabbitmq.WithConsumerOptionsExchangeName("events"),
        rabbitmq.WithConsumerOptionsExchangeDeclare,
    )
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close(ctx)
    
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
```

## 🚀 Quick Start Publisher

### With options

```go
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
    )
    if err != nil {
        log.Fatal(err)
    }
    defer publisher.Close(ctx)
    
    publisher.NotifyReturn(func(r rabbitmq.Return) {
        log.Printf("message returned from server: %s", string(r.Body))
    })
    
    publisher.NotifyPublish(func(c rabbitmq.Confirmation) {
        log.Printf("message confirmed from server. tag: %v, ack: %v", c.DeliveryTag, c.Ack)
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
                    log.Println(err)
                }
            case <-done:
            fmt.Println("stopping publisher")
            return
        }
    }
}

```

## Other usage examples

See the [examples](examples) directory for more ideas.

## Stability

Note that the API is currently in `v0`. I don't plan on any huge changes, but there may be some small breaking changes before we hit `v1`.

Submit an issue (above in the issues tab)

## Transient Dependencies

My goal is to keep dependencies limited to 1, [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go).

## 👏 Contributing

I love help! Contribute by forking the repo and opening pull requests. Please ensure that your code passes the existing tests and linting, and write tests to test your changes if applicable.

All pull requests should be submitted to the `main` branch.
