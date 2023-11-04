package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jxo-me/rabbitmq-go/internal/channelmanager"
	"github.com/jxo-me/rabbitmq-go/internal/connectionmanager"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// chanSendWaitTime is the maximum time we will wait when sending a
	// response, confirm or error on the corresponding channels. This is so that
	// we won't block forever if the listening goroutine has stopped listening.
	chanSendWaitTime = 10 * time.Second
)

type OnStartedFunc func(inputConn, outputConn *amqp.Connection, inputChannel, outputChannel *amqp.Channel)

type RpcClient struct {
	conn                       *Conn
	chanManager                *channelmanager.ChannelManager
	connManager                *connectionmanager.ConnectionManager
	disablePublishDueToFlow    bool
	disablePublishDueToFlowMux *sync.RWMutex

	disablePublishDueToBlocked    bool
	disablePublishDueToBlockedMux *sync.RWMutex

	options ClientOptions
	log     Logger

	// timeout is the time we should wait after a request is published before
	// we assume the request got lost.
	timeout time.Duration

	// maxRetries is the amount of times a request will be retried before
	// giving up.
	maxRetries int

	// requests is a single channel used whenever we want to publish a message.
	// The channel is consumed in a separate go routine which allows us to add
	// messages to the channel that we don't want replies from without the need
	// to wait for on going requests.
	requests chan *Request

	// replyToQueueName can be used to avoid generating queue names on the
	// message bus and use a pre-defined name throughout the usage of a client.
	replyToQueueName string

	// middlewares holds slice of middlewares to run before or after the client
	// sends a request.
	middlewares []ClientMiddlewareFunc

	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}

	// didStopChan will close when the client has finished shutdown.
	didStopChan chan struct{}

	// isRunning is one when the server is running.
	isRunning int32
	// wantStop tells the runForever function to exit even on connection errors.
	wantStop int32
	// requestsMap will keep track of requests waiting for confirmations,
	// replies or returns. it maps each correlation ID and delivery tag of a
	// message to the actual Request. This is to ensure that no matter the order
	// of a request and response, we will always publish the response to the
	// correct consumer.
	requestsMap RequestMap
	// Sender is the main send function called after all middlewares has been
	// chained and called.
	// This field can be overridden to simplify testing.
	Sender SendFunc
	// onStarted will all be executed after the client has connected.
	onStarted []OnStartedFunc
}

/*
OnStarted can be used to hook into the connections/channels that the client is
using. This can be useful if you want more control over amqp directly.
Note that since the client is lazy and won't connect until the first .Send()
the provided OnStartedFunc won't be called until then. Also note that this
is blocking and the client won't continue its startup until this function has
finished executing.

	client := NewClient(url)
	client.OnStarted(func(inConn, outConn *amqp.Connection, inChan, outChan *amqp.Channel) {
		// Do something with amqp connections/channels.
	})
*/
func (c *RpcClient) OnStarted(f OnStartedFunc) {
	c.onStarted = append(c.onStarted, f)
}

// respondErrorToRequest will return the provided response to the caller.
func (c *RpcClient) respondToRequest(ctx context.Context, request *Request, response *amqp.Delivery) {
	select {
	case request.response <- response:
		return
	case <-time.After(chanSendWaitTime):
		c.log.Errorf(ctx,
			"client: nobody is waiting for response on: %s, response: %s",
			stringifyRequestForLog(request),
			stringifyDeliveryForLog(response),
		)
	}
}

// respondErrorToRequest will return the provided error to the caller.
func (c *RpcClient) respondErrorToRequest(ctx context.Context, request *Request, err error) {
	select {
	case request.errChan <- err:
		return
	case <-time.After(chanSendWaitTime):
		c.log.Errorf(ctx,
			"nobody is waiting for error on: %s, error: %s",
			stringifyRequestForLog(request),
			err.Error(),
		)
	}
}

// confirmRequest will mark the provided request as confirmed by the amqp
// server.
func (c *RpcClient) confirmRequest(ctx context.Context, request *Request) {
	select {
	case request.confirmed <- struct{}{}:
		return
	case <-time.After(chanSendWaitTime):
		c.log.Errorf(ctx, "nobody is waiting for confirmation on: %s", stringifyRequestForLog(request))
	}
}

// retryRequest will retry the provided request, unless the request already
// has been retried too many times.
// Then the provided error will be sent to the
// caller instead.
func (c *RpcClient) retryRequest(ctx context.Context, request *Request, err error) {
	if request.numRetries >= c.maxRetries {
		// We have already retried too many times
		c.log.Errorf(ctx,
			"client: could not publish, giving up: reason: %s, %s",
			err.Error(),
			stringifyRequestForLog(request),
		)

		// We shouldn't wait for confirmations anymore because they will never
		// arrive.
		c.confirmRequest(ctx, request)

		// Return whatever error .Publish returned to the caller.
		c.respondErrorToRequest(ctx, request, err)

		return
	}

	request.numRetries++

	go func() {
		c.log.Debugf(ctx, "client: queuing request for retry: reason: %s, %s", err.Error(), stringifyRequestForLog(request))

		select {
		case c.requests <- request:
		case <-request.AfterTimeout():
			c.log.Errorf(ctx,
				"client: request timed out while waiting for retry reason: %s, %s",
				err.Error(),
				stringifyRequestForLog(request),
			)
		}
	}()
}

// runPublisher consumes messages from chan requests and publishes them on the
// amqp exchange.
// The method will stop consuming if the underlying amqp channel
// is closed for any reason, and when this happens, the messages will be put back
// in chan requests unless we have retried to many times.
func (c *RpcClient) runPublisher(ctx context.Context, ouputChan *amqp.Channel) {
	c.log.Debugf(ctx, "client: running publisher...")

	// Monitor the closing of this channel.
	// We need to do this in a separate,
	// goroutine to ensure we won't get a deadlock inside the select below
	// which can itself close this channel.
	onClose := make(chan struct{})

	go func() {
		<-ouputChan.NotifyClose(make(chan *amqp.Error))
		close(onClose)
	}()

	// Delivery tags always start at 1, but we increase it before we do any
	// .Publish() on the channel.
	nextDeliveryTag := uint64(0)

	for {
		select {
		case <-onClose:
			// The channels for publishing responses were closed, once the
			// client has started again.
			// This loop will be restarted.
			c.log.Debugf(ctx, "client: publisher stopped after the channel was closed")
			return
		case request := <-c.requests:
			// Set the ReplyTo if needed, or ensure it's empty if it's not.
			if request.Reply {
				request.Publishing.ReplyTo = c.replyToQueueName
			} else {
				request.Publishing.ReplyTo = ""
			}

			c.log.Debugf(ctx, "client: publishing %s", request.Publishing.CorrelationId)

			// Set up the delivery tag for this request.
			nextDeliveryTag++
			request.deliveryTag = nextDeliveryTag

			// Ensure the replies, returns and confirms; consumers can get a hold
			// of this request once they come in.
			c.requestsMap.Set(request)

			err := ouputChan.PublishWithContext(
				context.Background(),
				request.Exchange,
				request.RoutingKey,
				true,
				false,
				request.Publishing,
			)
			if err != nil {
				_ = ouputChan.Close()

				c.retryRequest(ctx, request, err)

				c.log.Errorf(ctx,
					"client: publisher stopped because of error: %s, request: %s",
					err.Error(),
					stringifyRequestForLog(request),
				)

				return
			}

			if !c.options.PublishOptions.ConfirmMode {
				// We're not in confirm mode, so we confirm that we have sent
				// the request here.
				c.confirmRequest(ctx, request)

				if !request.Reply {
					// Since we won't get a confirmation of this request, and
					// we don't want to have a reply, just return nil to the
					// caller.
					c.respondToRequest(ctx, request, nil)
				}
			}
		}
	}
}

// runConfirmsConsumer will consume both confirmations and returns, and since
// returns always arrive before confirmations, we want to finish handling any
// return before we handle any confirmations.
func (c *RpcClient) runConfirmsConsumer(ctx context.Context, confirms chan amqp.Confirmation, returns chan amqp.Return) {
	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				return
			}

			request, ok := c.requestsMap.GetByCorrelationID(ret.CorrelationId)
			if !ok {
				// This could happen if we stop waiting for requests to return due
				// to a timeout.
				// But since returns are normally rapid, that
				// would mean that something isn't quite right on the amqp server.
				c.log.Errorf(ctx, "client: got return for unknown request: %s", stringifyReturnForLog(ret))
				continue
			}

			c.log.Debugf(ctx, "client: publishing is returned by server: %s", ret.CorrelationId)

			request.returned = &ret

		case confirm, ok := <-confirms:
			if !ok {
				return
			}

			request, ok := c.requestsMap.GetByDeliveryTag(confirm.DeliveryTag)
			if !ok {
				// This could happen if we stop waiting for requests to return due
				// to a timeout. But since confirmations are normally rapid, that
				// would mean that something isn't quite right on the amqp server.
				// Unfortunately, there isn't any way of getting more information
				// than the delivery tag from a confirmation.
				c.log.Errorf(ctx, "client: got confirmation of unknown request: %d", confirm.DeliveryTag)

				continue
			}

			c.log.Debugf(ctx, "client: confirming request %s", request.Publishing.CorrelationId)

			c.confirmRequest(ctx, request)

			if !confirm.Ack {
				c.respondErrorToRequest(ctx, request, ErrRequestRejected)

				// Doesn't matter if the request wants the nil reply below because
				// we gave it an error instead.
				continue
			}

			// Check if the request was also returned.
			if request.returned != nil {
				c.respondErrorToRequest(
					ctx,
					request,
					fmt.Errorf("%w: %d, %s",
						ErrRequestReturned,
						request.returned.ReplyCode,
						request.returned.ReplyText,
					),
				)

				continue
			}

			if !request.Reply {
				// The request isn't expecting a reply, so we need give a nil
				// response instead to signal that we're done.
				c.log.Debugf(ctx,
					"client: sending nil response after confirmation due to no reply wanted %s",
					request.Publishing.CorrelationId,
				)

				c.respondToRequest(ctx, request, nil)
			}
		}
	}
}

// runRepliesConsumer will declare and start consuming from the queue where we
// expect replies to come back.
// The method will stop consuming if the
// underlying amqp channel is closed for any reason.
func (c *RpcClient) runRepliesConsumer(ctx context.Context, inChan *amqp.Channel) error {
	queue, err := inChan.QueueDeclare(
		c.replyToQueueName,
		false,
		false,
		false,
		false, // no-wait.
		map[string]interface{}{
			// Ensure the queue is deleted automatically when it's unused for
			// more than the set time.
			// This is to ensure that messages that
			// are in flight during a reconnecting don't get lost (which might
			// happen when using `DeleteWhenUnused`).
			"x-expires": 1 * 60 * 1000, // 1 minute.
		},
	)
	if err != nil {
		return err
	}

	messages, err := inChan.Consume(
		queue.Name,
		"",
		true,
		true,
		false, // no-local.
		false, // no-wait.
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		c.log.Debugf(ctx, "client: running replies consumer...")

		for response := range messages {
			request, ok := c.requestsMap.GetByCorrelationID(response.CorrelationId)
			if !ok {
				c.log.Errorf(ctx,
					"client: could not find where to reply. CorrelationId: %s",
					stringifyDeliveryForLog(&response),
				)

				continue
			}

			c.log.Debugf(ctx, "client: forwarding reply %s", response.CorrelationId)

			responseCopy := response

			select {
			case request.response <- &responseCopy:
			case <-time.After(chanSendWaitTime):
				c.log.Errorf(ctx, "client: could not send to reply response chan: %s", stringifyRequestForLog(request))
			}
		}

		c.log.Debugf(ctx, "client: replies consumer is done")
	}()

	return nil
}

// AddMiddleware will add a middleware which will be executed on request.
func (c *RpcClient) AddMiddleware(m ClientMiddlewareFunc) *RpcClient {
	c.middlewares = append(c.middlewares, m)

	return c
}

// runOnce will connect amqp, set up all the amqp channels, run the publisher
// and run the reply consumer.
// The method will also return the underlying
// amqp error if the underlying connection or socket isn't gracefully closed.
// It will also block until the connection is gone.
func (c *RpcClient) runOnce(ctx context.Context) error {
	c.log.Debugf(ctx, "client: starting up...")

	inputConn, outputConn, err := createConnections(ctx, c.connManager)
	if err != nil {
		return err
	}

	defer func() { _ = inputConn.Close() }()
	defer func() { _ = outputConn.Close() }()

	inputCh, outputCh, err := createChannels(inputConn, outputConn)
	if err != nil {
		return err
	}

	defer func() { _ = inputCh.Close() }()
	defer func() { _ = outputCh.Close() }()

	// Notify everyone that the client has started.
	// Runs sequentially, so there
	// aren't any race conditions when working with the connections or channels.
	for _, onStarted := range c.onStarted {
		onStarted(inputConn, outputConn, inputCh, outputCh)
	}

	err = c.runRepliesConsumer(ctx, inputCh)
	if err != nil {
		return err
	}

	if c.options.PublishOptions.ConfirmMode {
		// ConfirmMode is wanted, tell the amqp-server that we want to enable
		// confirm-mode on this channel and start the confirmation consumer.
		err = outputCh.Confirm(
			false, // no-wait.
		)

		if err != nil {
			return err
		}

		go c.runConfirmsConsumer(ctx,
			outputCh.NotifyPublish(make(chan amqp.Confirmation)),
			outputCh.NotifyReturn(make(chan amqp.Return)),
		)
	}

	go c.runPublisher(ctx, outputCh)

	err = monitorAndWait(
		c.stopChan,
		inputConn.NotifyClose(make(chan *amqp.Error)),
		outputConn.NotifyClose(make(chan *amqp.Error)),
		inputCh.NotifyClose(make(chan *amqp.Error)),
		outputCh.NotifyClose(make(chan *amqp.Error)),
	)

	if err != nil {
		return err
	}

	return nil
}

func (c *RpcClient) runForever(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 0, 1) {
		// Already running.
		return
	}
	// Always assume that we don't want to stop initially.
	atomic.StoreInt32(&c.wantStop, 0)

	c.stopChan = make(chan struct{})
	c.didStopChan = make(chan struct{})
	go func() {
		for {
			c.log.Debugf(ctx, "client: connecting...")

			err := c.runOnce(ctx)
			if err == nil {
				c.log.Debugf(ctx, "client: finished gracefully")
				break
			}

			if atomic.LoadInt32(&c.wantStop) == 1 {
				c.log.Debugf(ctx, "client: finished with error %s", err.Error())
				break
			}

			c.log.Errorf(ctx, "client: got error: %s, will reconnect in %v second(s)", err, 0.5)
			time.Sleep(500 * time.Millisecond)
		}

		// Tell c.Close() that we have finished shutdown and that it can return.
		close(c.didStopChan)

		// Ensure we can start again.
		atomic.StoreInt32(&c.isRunning, 0)
	}()
}

// Send will send a Request by using an amqp.Publishing.
func (c *RpcClient) Send(r *Request) (*amqp.Delivery, error) {
	//nolint:critic // We don't want to overwrite any slice, so it's
	// intentional to store an appended result in new slice.
	middlewares := append(c.middlewares, r.middlewares...)

	return ClientMiddlewareChain(c.Sender, middlewares...)(r.Context, r)
}

func (c *RpcClient) send(ctx context.Context, r *Request) (*amqp.Delivery, error) {
	// Ensure that the publisher is running.
	c.runForever(ctx)

	// This is where we get the responses back.
	// If this request doesn't want a reply back (by setting Reply to false)
	// this channel will get a nil message after the publisher has Published the
	// message.
	r.response = make(chan *amqp.Delivery)

	// This channel is sent to when the request is confirmed. This can happen
	// both when confirm-mode is set. And if not set, it's automatically
	// confirmed once the request is published.
	r.confirmed = make(chan struct{})

	// This is where we get any (client) errors if they occur before we could
	// even send the request.
	r.errChan = make(chan error)

	// Set the correlation id on the publishing if not yet set.
	if r.Publishing.CorrelationId == "" {
		r.Publishing.CorrelationId = uuid.New().String()
	}

	defer c.requestsMap.Delete(r)

	r.startTimeout(c.timeout)
	timeoutChan := r.AfterTimeout()

	c.log.Debugf(ctx, "client: queuing request %s", r.Publishing.CorrelationId)

	select {
	case c.requests <- r:
		// successful send.
	case <-timeoutChan:
		c.log.Debugf(ctx, "client: timeout while waiting for request queue %s", r.Publishing.CorrelationId)
		return nil, fmt.Errorf("%w while waiting for request queue", ErrRequestTimeout)
	}

	c.log.Debugf(ctx, "client: waiting for reply of %s", r.Publishing.CorrelationId)

	// We the app froze here until the request has been published (or when confirm-mode
	// is on; confirmed).
	select {
	case <-r.confirmed:
		// got confirmation.
	case <-timeoutChan:
		c.log.Debugf(ctx, "client: timeout while waiting for request confirmation %s", r.Publishing.CorrelationId)
		return nil, fmt.Errorf("%w while waiting for confirmation", ErrRequestTimeout)
	}

	// All responses are published on the request response channel.
	// The app froze here
	// until a response is received and closes the channel when it's read.
	select {
	case err := <-r.errChan:
		c.log.Debugf(ctx, "client: error for %s, %s", r.Publishing.CorrelationId, err.Error())
		return nil, err

	case <-timeoutChan:
		c.log.Debugf(ctx, "client: timeout for %s", r.Publishing.CorrelationId)
		return nil, fmt.Errorf("%w while waiting for response", ErrRequestTimeout)

	case delivery := <-r.response:
		c.log.Debugf(ctx, "client: got delivery for %s", r.Publishing.CorrelationId)
		return delivery, nil
	}
}

// Stop will gracefully disconnect from AMQP. It is not guaranteed that all
// in flight requests or responses are handled before the disconnect. Instead,
// the user should ensure that all calls to c.Send() has returned before calling
// c.Stop().
func (c *RpcClient) Stop() {
	if atomic.LoadInt32(&c.isRunning) != 1 {
		return
	}

	atomic.StoreInt32(&c.wantStop, 1)

	close(c.stopChan)
	<-c.didStopChan
}

func NewRpcClient(ctx context.Context, conn *Conn, optionFuncs ...func(*ClientOptions)) (*RpcClient, error) {
	replyQueueName := "rpc-reply-to-" + uuid.New().String()
	defaultOptions := getDefaultClientOptions(replyQueueName)
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

	rpcClient := &RpcClient{
		conn:                          conn,
		chanManager:                   chanManager,
		connManager:                   conn.connectionManager,
		disablePublishDueToFlow:       false,
		disablePublishDueToFlowMux:    &sync.RWMutex{},
		disablePublishDueToBlocked:    false,
		disablePublishDueToBlockedMux: &sync.RWMutex{},
		options:                       *options,
		log:                           options.Logger,
		maxRetries:                    10,
		requests:                      make(chan *Request),
		requestsMap: RequestMap{
			byDeliveryTag:   make(map[uint64]*Request),
			byCorrelationID: make(map[string]*Request),
		},
		middlewares:      []ClientMiddlewareFunc{},
		timeout:          time.Second * 10,
		replyToQueueName: replyQueueName,
	}

	rpcClient.Sender = rpcClient.send

	return rpcClient, nil
}

// PublishWithContext rpc request the provided data to the given routing key over the connection.
func (c *RpcClient) PublishWithContext(
	ctx context.Context,
	data []byte,
	routingKey string,
) ([]byte, error) {
	c.disablePublishDueToFlowMux.RLock()
	defer c.disablePublishDueToFlowMux.RUnlock()
	if c.disablePublishDueToFlow {
		return nil, fmt.Errorf("publishing blocked due to high flow on the server")
	}

	c.disablePublishDueToBlockedMux.RLock()
	defer c.disablePublishDueToBlockedMux.RUnlock()
	if c.disablePublishDueToBlocked {
		return nil, fmt.Errorf("publishing blocked due to TCP block on the server")
	}

	request := NewRequest(ctx).WithRoutingKey(routingKey).WithBody(data)
	response, err := c.Send(request)
	if err != nil {
		return nil, err
	}
	return response.Body, err
}

// Close closes the publisher and releases resources
// The publisher should be discarded as it's not safe for re-use
// Only call Close() once
func (c *RpcClient) Close(ctx context.Context) {
	c.Stop()
	// close the channel so that rabbitmq server knows that the
	// publisher has been stopped.
	err := c.chanManager.Close(ctx)
	if err != nil {
		c.options.Logger.Warningf(ctx, "error while closing the channel: %v", err)
	}
	c.options.Logger.Infof(ctx, "closing publisher...")
}
