package rabbitmq

import "time"

// ExchangeOptions are used to configure an exchange.
// If the Passive flag is set the client will only check if the exchange exists on the server
// and that the settings match, no creation attempt will be made.
type ExchangeOptions struct {
	Name       string
	Kind       string // possible values: empty string for default exchange or direct, topic, fanout
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Passive    bool // if false, a missing exchange will be created on the server
	Args       Table
	Declare    bool
	Bindings   []Binding
}

// QueueOptions are used to configure a queue.
// A passive queue is assumed by RabbitMQ to already exist, and attempting to connect
// to a non-existent queue will cause RabbitMQ to throw an exception.
type QueueOptions struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Passive    bool // if false, a missing queue will be created on the server
	Args       Table
	Declare    bool
}

// Binding describes the binding of a queue to a routing key on an exchange
type Binding struct {
	RoutingKey string
	BindingOptions
}

// BindingOptions describes the options a binding can have
type BindingOptions struct {
	NoWait  bool
	Args    Table
	Declare bool
}

// ConsumeOptions are used to configure the consumer
// on the rabbit server
type ConsumeOptions struct {
	Name      string
	AutoAck   bool
	Exclusive bool
	NoWait    bool
	NoLocal   bool
	Args      Table
}

// PublishOptions are used to control how data is published
type PublishOptions struct {
	Exchange string
	// Mandatory fails to publish if there are no queues
	// bound to the routing key
	Mandatory bool
	// Immediate fails to publish if there are no consumers
	// that can ack bound to the queue on the routing key
	Immediate bool
	// MIME content type
	ContentType string
	// Transient (0 or 1) or Persistent (2)
	DeliveryMode uint8
	// Expiration time in ms that a message will expire from a queue.
	// See https://www.rabbitmq.com/ttl.html#per-message-ttl-in-publishers
	Expiration string
	// MIME content encoding
	ContentEncoding string
	// 0 to 9
	Priority uint8
	// correlation identifier
	CorrelationID string
	// address to reply to (ex: RPC)
	ReplyTo string
	// message identifier
	MessageID string
	// message timestamp
	Timestamp time.Time
	// message type name
	Type string
	// creating user id - ex: "guest"
	UserID string
	// creating application id
	AppID string
	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	Headers Table
}
