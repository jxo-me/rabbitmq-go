package rabbitmq

import (
	"github.com/jxo-me/rabbitmq-go/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type ClientOptions struct {
	ConsumeOptions  ConsumeOptions
	QueueOptions    QueueOptions
	ExchangeOptions ExchangeOptions
	PublishOptions  PublishOptions
	Logger          logger.Logger
	// ConfirmMode puts the channel that messages are published over in
	// confirmation mode.
	// This makes sending requests more reliable at the cost
	// of some performance.
	// The server must confirm each publishing.
	// See https://www.rabbitmq.com/confirms.html#publisher-confirms
	ConfirmMode bool
}

// getDefaultClientOptions describes the options that will be used when a value isn't provided
func getDefaultClientOptions(queueName string) ClientOptions {
	return ClientOptions{
		ConsumeOptions: ConsumeOptions{
			Name:      "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
			Args:      Table{},
		},
		QueueOptions: QueueOptions{
			Name:       queueName,
			Durable:    false,
			AutoDelete: false,
			Exclusive:  false,
			NoWait:     false,
			Passive:    false,
			Declare:    true,
			Args: map[string]interface{}{
				// Ensure the queue is deleted automatically when it's unused for
				// more than the set time. This is to ensure that messages that
				// are in flight during a reconnecting don't get lost (which might
				// happen when using `DeleteWhenUnused`).
				"x-expires": 1 * 60 * 1000, // 1 minute.
			},
		},
		ExchangeOptions: ExchangeOptions{
			Name:       "",
			Kind:       amqp.ExchangeDirect,
			Durable:    false,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Passive:    false,
			Declare:    false,
			Args:       Table{},
		},
		Logger: stdDebugLogger{},
	}
}

// WithClientOptionsLogging sets logging to true on the client options
// and sets the
func WithClientOptionsLogging(options *ClientOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithClientOptionsLogger sets logging to a custom interface.
// Use WithClientOptionsLogging to just log to stdout.
func WithClientOptionsLogger(log Logger) func(options *ClientOptions) {
	return func(options *ClientOptions) {
		options.Logger = log
	}
}

// WithClientPublishOptionsMandatory makes the publishing mandatory, which means when a queue is not
// bound to the routing key, a message will be sent back on the return channel for you to handle
func WithClientPublishOptionsMandatory(options *ClientOptions) {
	options.PublishOptions.Mandatory = true
}

// WithClientPublishOptionsImmediate makes the publishing immediate, which means when a consumer is not available
// to immediately handle the new message, a message will be sent back on the return channel for you to handle
func WithClientPublishOptionsImmediate(options *ClientOptions) {
	options.PublishOptions.Immediate = true
}

// WithClientOptionsConsumerName returns a function that sets the name on the server of this consumer
// if unset a random name will be given
func WithClientOptionsConsumerName(consumerName string) func(*ClientOptions) {
	return func(options *ClientOptions) {
		options.ConsumeOptions.Name = consumerName
	}
}

// WithClientOptionsConsumerAutoAck returns a function
// that sets the auto acknowledge property on the server of this consumer
// if unset, the default will be used (false)
func WithClientOptionsConsumerAutoAck(autoAck bool) func(*ClientOptions) {
	return func(options *ClientOptions) {
		options.ConsumeOptions.AutoAck = autoAck
	}
}

// WithClientOptionsConsumerExclusive sets the consumer to exclusive, which means
// the server will ensure that this is the sole consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func WithClientOptionsConsumerExclusive(options *ClientOptions) {
	options.ConsumeOptions.Exclusive = true
}

// WithClientOptionsQueueDurable ensures the queue is a durable queue
func WithClientOptionsQueueDurable(options *ClientOptions) {
	options.QueueOptions.Durable = true
}

// WithClientOptionsQueueAutoDelete ensures the queue is an auto-delete queue
func WithClientOptionsQueueAutoDelete(options *ClientOptions) {
	options.QueueOptions.AutoDelete = true
}

// WithClientOptionsQueueExclusive ensures the queue is an exclusive queue
func WithClientOptionsQueueExclusive(options *ClientOptions) {
	options.QueueOptions.Exclusive = true
}

// WithClientOptionsQueueArgs adds optional args to the queue
func WithClientOptionsQueueArgs(args Table) func(*ClientOptions) {
	return func(options *ClientOptions) {
		options.QueueOptions.Args = args
	}
}

// WithLogger sets the logger to use for error logging.
func (c *RpcClient) WithLogger(f Logger) *RpcClient {
	c.log = f

	return c
}

// WithQueueDeclareSettings will set the settings used when declaring queues
// for the client globally.
func (c *RpcClient) WithQueueDeclareSettings(s QueueOptions) *RpcClient {
	c.options.QueueOptions = s

	return c
}

// WithConsumeSettings will set the settings used when consuming in the client
// globally.
func (c *RpcClient) WithConsumeSettings(s ConsumeOptions) *RpcClient {
	c.options.ConsumeOptions = s

	return c
}

// WithPublishSettings will set the client publishing settings when publishing
// messages.
func (c *RpcClient) WithPublishSettings(s PublishOptions) *RpcClient {
	c.options.PublishOptions = s

	return c
}

// WithConfirmMode sets the confirm-mode on the client. This causes the client
// to wait for confirmations, and if none arrives or the confirmation is marked
// as Nack, Client#Send() returns a corresponding error.
func (c *RpcClient) WithConfirmMode(confirmMode bool) *RpcClient {
	c.options.ConfirmMode = confirmMode

	return c
}

// WithTimeout will set the client timeout used when publishing messages.
// T will be rounded using the duration's Round function to the nearest
// multiple of a millisecond. Rounding will be away from zero.
func (c *RpcClient) WithTimeout(t time.Duration) *RpcClient {
	c.timeout = t.Round(time.Millisecond)

	return c
}

// WithMaxRetries sets the maximum number of times, the client will retry
// sending the request before giving up and returning the error to the caller
// of c.Send(). This retry will persist during reconnecting.
func (c *RpcClient) WithMaxRetries(n int) *RpcClient {
	c.maxRetries = n

	return c
}
