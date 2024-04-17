package rabbitmq

import (
	"time"
)

// WithPublishOptionsExchange returns a function that sets the exchange to publish to
func WithPublishOptionsExchange(exchange string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Exchange = exchange
	}
}

// WithPublishOptionsMandatory makes the publishing mandatory, which means when a queue is not
// bound to the routing key a message will be sent back on the returns channel for you to handle
func WithPublishOptionsMandatory(options *PublishOptions) {
	options.Mandatory = true
}

// WithPublishOptionsImmediate makes the publishing immediate, which means when a consumer is not available
// to immediately handle the new message, a message will be sent back on the returns channel for you to handle
func WithPublishOptionsImmediate(options *PublishOptions) {
	options.Immediate = true
}

// WithPublishOptionsContentType returns a function that sets the content type, i.e. "application/json"
func WithPublishOptionsContentType(contentType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentType = contentType
	}
}

// WithPublishOptionsPersistentDelivery sets the message to persist. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart. By default publishings
// are transient
func WithPublishOptionsPersistentDelivery(options *PublishOptions) {
	options.DeliveryMode = Persistent
}

// WithPublishOptionsExpiration returns a function that sets the expiry/TTL of a message. As per RabbitMq spec, it must be a
// string value in milliseconds.
func WithPublishOptionsExpiration(expiration string) func(options *PublishOptions) {
	return func(options *PublishOptions) {
		options.Expiration = expiration
	}
}

// WithPublishOptionsHeaders returns a function that sets message header values, i.e. "msg-id"
func WithPublishOptionsHeaders(headers Table) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Headers = headers
	}
}

// WithPublishOptionsContentEncoding returns a function that sets the content encoding, i.e. "utf-8"
func WithPublishOptionsContentEncoding(contentEncoding string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentEncoding = contentEncoding
	}
}

// WithPublishOptionsPriority returns a function that sets the content priority from 0 to 9
func WithPublishOptionsPriority(priority uint8) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Priority = priority
	}
}

// WithPublishOptionsCorrelationID returns a function that sets the content correlation identifier
func WithPublishOptionsCorrelationID(correlationID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.CorrelationID = correlationID
	}
}

// WithPublishOptionsReplyTo returns a function that sets the reply to field
func WithPublishOptionsReplyTo(replyTo string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ReplyTo = replyTo
	}
}

// WithPublishOptionsMessageID returns a function that sets the message identifier
func WithPublishOptionsMessageID(messageID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.MessageID = messageID
	}
}

// WithPublishOptionsTimestamp returns a function that sets the timestamp for the message
func WithPublishOptionsTimestamp(timestamp time.Time) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Timestamp = timestamp
	}
}

// WithPublishOptionsType returns a function that sets the message type name
func WithPublishOptionsType(messageType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Type = messageType
	}
}

// WithPublishOptionsUserID returns a function that sets the user id i.e. "user"
func WithPublishOptionsUserID(userID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.UserID = userID
	}
}

// WithPublishOptionsAppID returns a function that sets the application id
func WithPublishOptionsAppID(appID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.AppID = appID
	}
}
