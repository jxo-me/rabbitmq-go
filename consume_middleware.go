package rabbitmq

/*
ConsumeMiddlewareFunc represent a function that can be used as middleware.

For example:

	func myMiddle(next HandlerFunc) HandlerFunc {

		// Preinitialization of middleware here.

		return func(ctx context.Context, rw *ResponseWriter d amqp.Delivery) {
			// Before handler execution here.

			// Execute the handler.
			next(ctx, rw, d)

			// After execution here.
		}
	}

	s := New("url")

	// Add middleware to specific handler.
	s.Bind(DirectBinding("foobar", myMiddle(HandlerFunc)))

	// Add middleware to all handlers on the server.
	s.AddMiddleware(myMiddle)
*/
type ConsumeMiddlewareFunc func(next HandlerFunc) HandlerFunc

/*
ConsumeMiddlewareChain will attach all given middlewares to your HandlerFunc.
The middlewares will be executed in the same order as your input.

For example:

	s := New("url")

	s.Bind(DirectBinding(
		"foobar",
		ServerMiddlewareChain(
			myHandler,
			middlewareOne,
			middlewareTwo,
			middlewareThree,
		),
	))
*/
func ConsumeMiddlewareChain(next HandlerFunc, m ...ConsumeMiddlewareFunc) HandlerFunc {
	if len(m) == 0 {
		// The middleware chain is done. All middlewares have been applied.
		return next
	}

	// Nest the middlewares so that we attatch them in order.
	// The first middleware will have the second middleware applied, and so on.
	return m[0](ConsumeMiddlewareChain(next, m[1:]...))
}
