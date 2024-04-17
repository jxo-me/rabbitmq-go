package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jxo-me/rabbitmq-go/internal/channelmanager"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Action is an action that occurs after processed this delivery
type Action int

// Handler defines the handler of each Delivery and return Action
type Handler func(ctx context.Context, rw *ResponseWriter, d Delivery) (action Action)

// HandlerFunc is the function that handles all request based on the routing key.
type HandlerFunc func(context.Context, *ResponseWriter, amqp.Delivery)

const (
	// Ack default ack this msg after you have successfully processed this delivery.
	Ack Action = iota
	// NackDiscard the message will be dropped or delivered to a server configured dead-letter queue.
	NackDiscard
	// NackRequeue deliver this message to a different consumer.
	NackRequeue
	// Manual Message acknowledgement is left to the user using the msg.Ack() method
	Manual
)

// processedRequest is used to add the response from a handler func combined
// with an amqp.Delivery.
// The reason we need to combine this is that we reply
// to each request in a separate go routine and the delivery is required to
// determine on which queue to reply.
type processedRequest struct {
	replyTo    string
	mandatory  bool
	immediate  bool
	publishing amqp.Publishing
}

// Consumer allows you to create and connect to queues for data consumption.
type Consumer struct {
	chanManager                *channelmanager.ChannelManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}
	options                    ConsumerOptions

	isClosedMux *sync.RWMutex
	isClosed    bool
	outputCh    *amqp.Channel
	consumersWg sync.WaitGroup
	responderWg sync.WaitGroup
	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}

	// middlewares are chained and executed on request.
	middlewares []ConsumeMiddlewareFunc

	// Every processed request will be responded to in a separate go routine.
	// The server holds a chanel on which all the responses from a handler func
	// are added.
	responses chan processedRequest
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	amqp.Delivery
}

// NewConsumer returns a new Consumer connected to the given rabbitmq server
// it also starts consuming on the given connection with automatic reconnection handling
// Do not reuse the returned consumer for anything other than to close it
func NewConsumer(
	ctx context.Context,
	conn *Conn,
	handler Handler,
	queue string,
	optionFuncs ...func(*ConsumerOptions),
) (*Consumer, error) {
	defaultOptions := getDefaultConsumerOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(ctx, conn.connectionManager, options.Logger, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}
	reconnectErrCh, closeCh := chanManager.NotifyReconnect()

	outputCh, err := conn.GetNewChannel()
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		chanManager:                chanManager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
		isClosedMux:                &sync.RWMutex{},
		isClosed:                   false,
		outputCh:                   outputCh,
		stopChan:                   make(chan struct{}),
		middlewares:                []ConsumeMiddlewareFunc{},
		responses:                  make(chan processedRequest),
		consumersWg:                sync.WaitGroup{},
		responderWg:                sync.WaitGroup{},
	}
	consumer.consumersWg.Add(1) // Sync the waitgroup to this goroutine.
	err = consumer.startGoroutines(
		ctx,
		handler,
		*options,
	)
	if err != nil {
		return nil, err
	}
	// This WaitGroup will reach 0 when the responder() has finished sending
	// all responses.
	consumer.responderWg.Add(1) // Sync the waitgroup to this goroutine.
	go consumer.responder(ctx, consumer.outputCh, &consumer.responderWg)

	go func() {
		err = monitorAndWait(
			consumer.stopChan,
			consumer.outputCh.NotifyClose(make(chan *amqp.Error)),
		)
		if err != nil {
			consumer.options.Logger.Warningf(ctx, "consumer monitor and wait error: %s", err.Error())
		}
	}()

	go func() {
		for err := range consumer.reconnectErrCh {
			consumer.options.Logger.Infof(ctx, "successful consumer recovery from: %v", err)
			// Set up a WaitGroup for use by consume().
			// This WaitGroup will be 0
			// when all consumers are finished consuming messages.
			consumer.consumersWg = sync.WaitGroup{}
			consumer.consumersWg.Add(1) // Sync the waitgroup to this goroutine.
			err = consumer.startGoroutines(
				ctx,
				handler,
				*options,
			)
			if err != nil {
				consumer.options.Logger.Fatalf(ctx, "error restarting consumer goroutines after cancel or close: %v", err)
				consumer.options.Logger.Fatalf(ctx, "consumer closing, unable to recover")
				return
			}
			// This WaitGroup will reach 0 when the responder() has finished sending
			// all responses.
			consumer.responderWg = sync.WaitGroup{}
			consumer.responderWg.Add(1) // Sync the waitgroup to this goroutine.
			go consumer.responder(ctx, outputCh, &consumer.responderWg)
		}
	}()

	return consumer, nil
}

// Run starts consuming with automatic reconnection handling. Do not reuse the
// consumer for anything other than to close it.
func (consumer *Consumer) Run(ctx context.Context, handler Handler) error {
	err := consumer.startGoroutines(
		ctx,
		handler,
		consumer.options,
	)
	if err != nil {
		return err
	}

	for err = range consumer.reconnectErrCh {
		consumer.options.Logger.Infof(ctx, "successful consumer recovery from: %v", err)
		err = consumer.startGoroutines(
			ctx,
			handler,
			consumer.options,
		)
		if err != nil {
			return fmt.Errorf("error restarting consumer goroutines after cancel or close: %w", err)
		}
	}

	return nil
}

// Close cleans up resources and closes the consumer.
// It does not close the connection manager, just the subscription
// to the connection manager and the consuming goroutines.
// Only call once.
func (consumer *Consumer) Close(ctx context.Context) {
	consumer.isClosedMux.Lock()
	defer consumer.isClosedMux.Unlock()
	consumer.isClosed = true
	// close the channel so that rabbitmq server knows that the
	// consumer has been stopped.
	err := consumer.chanManager.Close(ctx)
	if err != nil {
		consumer.options.Logger.Warningf(ctx, "error while closing the channel: %v", err)
	}
	_ = consumer.outputCh.Close()
	// 3. We've told amqp to stop delivering messages, now we wait for all
	// the consumers to finish inflight messages.
	consumer.consumersWg.Done()
	consumer.consumersWg.Wait()
	// 4. Close the responses chan and wait until the consumers are finished.
	// We might still have responses we want to send.
	close(consumer.responses)
	consumer.responderWg.Done()
	consumer.responderWg.Wait()

	consumer.options.Logger.Infof(ctx, "closing consumer...")
	go func() {
		consumer.closeConnectionToManagerCh <- struct{}{}
	}()
}

func (consumer *Consumer) responder(ctx context.Context, outCh *amqp.Channel, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for response := range consumer.responses {
		consumer.options.Logger.Debugf(ctx,
			"server: publishing response to %s, correlation id: %s",
			response.replyTo, response.publishing.CorrelationId,
		)

		err := outCh.PublishWithContext(
			ctx,
			"", // exchange
			response.replyTo,
			response.mandatory,
			response.immediate,
			response.publishing,
		)
		if err != nil {
			// Close the channel so ensure reconnect.
			_ = outCh.Close()

			// We resend the response here so that other running goroutines
			// that have a working outCh can pick up this response.
			consumer.options.Logger.Errorf(ctx,
				"server: retrying publishing response to %s, reason: %s, response: %s",
				response.replyTo, err.Error(), stringifyPublishingForLog(response.publishing),
			)
			consumer.responses <- response

			return
		}
	}
}

// startGoroutines declares the queue if it doesn't exist,
// binds the queue to the routing key(s), and starts the goroutines
// that will consume from the queue
func (consumer *Consumer) startGoroutines(
	ctx context.Context,
	handler Handler,
	options ConsumerOptions,
) error {
	err := consumer.chanManager.QosSafe(
		options.QOSPrefetch,
		0,
		options.QOSGlobal,
	)
	if err != nil {
		return fmt.Errorf("declare qos failed: %w", err)
	}
	for _, exchangeOption := range options.ExchangeOptions {
		err = declareExchange(consumer.chanManager, exchangeOption)
		if err != nil {
			return fmt.Errorf("declare exchange failed: %w", err)
		}
	}
	err = declareQueue(consumer.chanManager, options.QueueOptions)
	if err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}
	err = declareBindings(consumer.chanManager, options)
	if err != nil {
		return fmt.Errorf("declare bindings failed: %w", err)
	}

	msgs, err := consumer.chanManager.ConsumeSafe(
		options.QueueOptions.Name,
		options.ConsumerOptions.Name,
		options.ConsumerOptions.AutoAck,
		options.ConsumerOptions.Exclusive,
		false, // no-RabbitMQ does not support local
		options.ConsumerOptions.NoWait,
		tableToAMQPTable(options.ConsumerOptions.Args),
	)
	if err != nil {
		return err
	}

	for i := 0; i < options.Concurrency; i++ {
		go handlerGoroutine(ctx, consumer, options.QueueOptions.Name, msgs, options, handler, &consumer.consumersWg)
	}
	consumer.options.Logger.Infof(ctx, "Processing messages on %v goroutines", options.Concurrency)
	return nil
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMux.RLock()
	defer consumer.isClosedMux.RUnlock()
	return consumer.isClosed
}

func handlerGoroutine(sCtx context.Context, consumer *Consumer, queueName string, msgs <-chan amqp.Delivery, consumeOptions ConsumerOptions, handler Handler, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for msg := range msgs {
		if consumer.getIsClosed() {
			break
		}
		// Add one delta to the wait group each time a delivery is handled so
		// we can end by marking it as done. This will ensure that we don't
		// close the response channel until the very last go routing handling
		// a delivery is finished even though we handle them concurrently.
		wg.Add(1)
		rw := ResponseWriter{
			Publishing: &amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body:          []byte{},
			},
		}
		ctx := context.Background()
		ctx = ContextWithShutdownChan(ctx, consumer.stopChan)
		ctx = ContextWithQueueName(ctx, queueName)
		delivery := Delivery{msg}
		if consumeOptions.ConsumerOptions.AutoAck {
			handler(ctx, &rw, delivery)
			if msg.ReplyTo != "" {
				consumer.responses <- processedRequest{
					replyTo:    msg.ReplyTo,
					mandatory:  rw.Mandatory,
					immediate:  rw.Immediate,
					publishing: *rw.Publishing,
				}
			}
			// Mark the specific delivery as finished.
			wg.Done()
			continue
		}
		switch handler(ctx, &rw, delivery) {
		case Ack:
			err := msg.Ack(false)
			if err != nil {
				consumer.options.Logger.Errorf(ctx, "can't ack message: %v", err)
			}
		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				consumer.options.Logger.Errorf(ctx, "can't nack message: %v", err)
			}
		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				consumer.options.Logger.Errorf(ctx, "can't nack message: %v", err)
			}
		}
		if msg.ReplyTo != "" {
			consumer.responses <- processedRequest{
				replyTo:    msg.ReplyTo,
				mandatory:  rw.Mandatory,
				immediate:  rw.Immediate,
				publishing: *rw.Publishing,
			}
		}
		// Mark the specific delivery as finished.
		wg.Done()
	}
	consumer.options.Logger.Infof(sCtx, "rabbit consumer goroutine closed")
}
