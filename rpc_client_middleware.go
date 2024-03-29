package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

// SendFunc represents the function that Send does. It takes a Request as input
// and returns a delivery and an error.
type SendFunc func(ctx context.Context, r *Request) (d *amqp.Delivery, e error)

// ClientMiddlewareFunc represents a function that can be used as middleware.
type ClientMiddlewareFunc func(next SendFunc) SendFunc

// ClientMiddlewareChain will attach all given middlewares to your SendFunc.
// The middlewares will be executed in the same order as your input.
func ClientMiddlewareChain(next SendFunc, m ...ClientMiddlewareFunc) SendFunc {
	if len(m) == 0 {
		return next
	}

	return m[0](ClientMiddlewareChain(next, m[1:]...))
}
