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
	c.options.PublishOptions.ConfirmMode = confirmMode

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
