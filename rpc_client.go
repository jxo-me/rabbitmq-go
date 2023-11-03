package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/jxo-me/rabbitmq-go/internal/channelmanager"
	"github.com/jxo-me/rabbitmq-go/internal/connectionmanager"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type RpcClient struct {
	chanManager                *channelmanager.ChannelManager
	connManager                *connectionmanager.ConnectionManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}

	disablePublishDueToFlow    bool
	disablePublishDueToFlowMux *sync.RWMutex

	disablePublishDueToBlocked    bool
	disablePublishDueToBlockedMux *sync.RWMutex

	handlerMux           *sync.Mutex
	notifyReturnHandler  func(r Return)
	notifyPublishHandler func(p Confirmation)

	options PublisherOptions
}

func (rpc *RpcClient) startup(ctx context.Context) error {
	err := declareExchange(rpc.chanManager, rpc.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
	go rpc.startNotifyFlowHandler(ctx)
	go rpc.startNotifyBlockedHandler(ctx)
	return nil
}

func NewRpcClient(ctx context.Context, conn *Conn, optionFuncs ...func(*PublisherOptions)) (*RpcClient, error) {
	defaultOptions := getDefaultPublisherOptions()
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
	rpcClient := &RpcClient{
		chanManager:                   chanManager,
		connManager:                   conn.connectionManager,
		reconnectErrCh:                reconnectErrCh,
		closeConnectionToManagerCh:    closeCh,
		disablePublishDueToFlow:       false,
		disablePublishDueToFlowMux:    &sync.RWMutex{},
		disablePublishDueToBlocked:    false,
		disablePublishDueToBlockedMux: &sync.RWMutex{},
		handlerMux:                    &sync.Mutex{},
		notifyReturnHandler:           nil,
		notifyPublishHandler:          nil,
		options:                       *options,
	}

	err = rpcClient.startup(ctx)
	if err != nil {
		return nil, err
	}

	if options.ConfirmMode {
		rpcClient.NotifyPublish(func(_ Confirmation) {
			// set a blank handler to set the channel in confirm mode
		})
	}

	go func() {
		for err := range rpcClient.reconnectErrCh {
			rpcClient.options.Logger.Infof(ctx, "successful publisher recovery from: %v", err)
			err := rpcClient.startup(ctx)
			if err != nil {
				rpcClient.options.Logger.Fatalf(ctx, "error on startup for publisher after cancel or close: %v", err)
				rpcClient.options.Logger.Fatalf(ctx, "publisher closing, unable to recover")
				return
			}
			rpcClient.startReturnHandler()
			rpcClient.startPublishHandler()
		}
	}()

	return rpcClient, nil
}

// RequestWithContext rpc request the provided data to the given routing key over the connection.
func (rpc *RpcClient) RequestWithContext(
	ctx context.Context,
	data []byte,
	routingKey string,
	optionFuncs ...func(*PublishOptions),
) ([]byte, error) {
	rpc.disablePublishDueToFlowMux.RLock()
	defer rpc.disablePublishDueToFlowMux.RUnlock()
	if rpc.disablePublishDueToFlow {
		return nil, fmt.Errorf("rpc request blocked due to high flow on the server")
	}

	rpc.disablePublishDueToBlockedMux.RLock()
	defer rpc.disablePublishDueToBlockedMux.RUnlock()
	if rpc.disablePublishDueToBlocked {
		return nil, fmt.Errorf("rpc request blocked due to TCP block on the server")
	}

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}
	msgs, err := rpc.chanManager.ConsumeSafe(
		routingKey,        // queue
		options.MessageID, // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	message := amqp.Publishing{}
	message.ContentType = options.ContentType
	message.DeliveryMode = options.DeliveryMode
	message.Body = data
	message.Headers = tableToAMQPTable(options.Headers)
	message.Expiration = options.Expiration
	message.ContentEncoding = options.ContentEncoding
	message.Priority = options.Priority
	message.CorrelationId = options.CorrelationID
	message.ReplyTo = options.ReplyTo
	message.MessageId = options.MessageID
	message.Timestamp = options.Timestamp
	message.Type = options.Type
	message.UserId = options.UserID
	message.AppId = options.AppID

	// Actual publish.
	err = rpc.chanManager.PublishWithContextSafe(
		ctx,
		options.Exchange,
		routingKey,
		options.Mandatory,
		options.Immediate,
		message,
	)
	if err != nil {
		return nil, err
	}
	for d := range msgs {
		if message.CorrelationId == d.CorrelationId {
			return d.Body, nil
		}
	}
	return nil, err
}

// Close closes the publisher and releases resources
// The publisher should be discarded as it's not safe for re-use
// Only call Close() once
func (rpc *RpcClient) Close(ctx context.Context) {
	// close the channel so that rabbitmq server knows that the
	// publisher has been stopped.
	err := rpc.chanManager.Close(ctx)
	if err != nil {
		rpc.options.Logger.Warningf(ctx, "error while closing the channel: %v", err)
	}
	rpc.options.Logger.Infof(ctx, "closing publisher...")
	go func() {
		rpc.closeConnectionToManagerCh <- struct{}{}
	}()
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
// These notifications are shared across an entire connection, so if you're creating multiple
// rpcRequest on the same connection keep that in mind
func (rpc *RpcClient) NotifyReturn(handler func(r Return)) {
	rpc.handlerMux.Lock()
	start := rpc.notifyReturnHandler == nil
	rpc.notifyReturnHandler = handler
	rpc.handlerMux.Unlock()

	if start {
		rpc.startReturnHandler()
	}
}

// NotifyPublish registers a listener for publish confirmations, must set ConfirmPublishings option
// These notifications are shared across an entire connection, so if you're creating multiple
// rpcRequest on the same connection keep that in mind
func (rpc *RpcClient) NotifyPublish(handler func(p Confirmation)) {
	rpc.handlerMux.Lock()
	shouldStart := rpc.notifyPublishHandler == nil
	rpc.notifyPublishHandler = handler
	rpc.handlerMux.Unlock()

	if shouldStart {
		rpc.startPublishHandler()
	}
}

func (rpc *RpcClient) startReturnHandler() {
	rpc.handlerMux.Lock()
	if rpc.notifyReturnHandler == nil {
		rpc.handlerMux.Unlock()
		return
	}
	rpc.handlerMux.Unlock()

	go func() {
		returns := rpc.chanManager.NotifyReturnSafe(make(chan amqp.Return, 1))
		for ret := range returns {
			go rpc.notifyReturnHandler(Return{ret})
		}
	}()
}

func (rpc *RpcClient) startPublishHandler() {
	rpc.handlerMux.Lock()
	if rpc.notifyPublishHandler == nil {
		rpc.handlerMux.Unlock()
		return
	}
	rpc.handlerMux.Unlock()
	_ = rpc.chanManager.ConfirmSafe(false)

	go func() {
		confirmationCh := rpc.chanManager.NotifyPublishSafe(make(chan amqp.Confirmation, 1))
		for conf := range confirmationCh {
			go rpc.notifyPublishHandler(Confirmation{
				Confirmation:      conf,
				ReconnectionCount: int(rpc.chanManager.GetReconnectionCount()),
			})
		}
	}()
}
