package rabbitmq

import "errors"

var (
	// ErrRequestReturned can be returned by Client#Send() when the server
	// returns the message. For example, when mandatory is set but the message
	// cannot be routed.
	ErrRequestReturned = errors.New("publishing returned")

	// ErrRequestRejected can be returned by Client#Send() when the server Backs
	// the message. This can happen if there is some problem inside the amqp
	// server. To check if the error returned is an ErrRequestReturned error, use
	// errors.Is(err, ErrRequestRejected).
	ErrRequestRejected = errors.New("publishing Backed")

	// ErrRequestTimeout is an error returned when a client request does not
	// receive a response within the client timeout duration. To check if the
	// error returned is an ErrRequestTimeout error, use errors.Is(err,
	// ErrRequestTimeout).
	ErrRequestTimeout = errors.New("request timed out")
	// ErrUnexpectedConnClosed is returned by ListenAndServe() if the server
	// shuts down without calling Stop() and if AMQP does not give an error
	// when said shutdown happens.
	ErrUnexpectedConnClosed = errors.New("unexpected connection close without specific error")
)
