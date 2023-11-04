package rabbitmq

import (
	"context"
	"github.com/jxo-me/rabbitmq-go/internal/connectionmanager"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Conn manages the connection to a rabbit cluster
// it is intended to be shared across publishers and consumers
type Conn struct {
	connectionManager          *connectionmanager.ConnectionManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}

	options ConnectionOptions
}

// Config wraps amqp.Config
// Config is used in DialConfig and Open to specify the desired tuning
// parameters used during a connection open handshake.  The negotiated tuning
// will be stored in the returned connection's Config field.
type Config amqp.Config

// NewConn creates a new connection manager
func NewConn(ctx context.Context, url string, optionFuncs ...func(*ConnectionOptions)) (*Conn, error) {
	defaultOptions := getDefaultConnectionOptions()
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	manager, err := connectionmanager.NewConnectionManager(ctx, url, amqp.Config(options.Config), options.Logger, options.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	reconnectErrCh, closeCh := manager.NotifyReconnect()
	conn := &Conn{
		connectionManager:          manager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
	}

	go conn.handleRestarts(ctx)
	return conn, nil
}

func (conn *Conn) handleRestarts(ctx context.Context) {
	for err := range conn.reconnectErrCh {
		conn.options.Logger.Infof(ctx, "successful connection recovery from: %v", err)
	}
}

// Close closes the connection, it's not safe for re-use.
// You should also close any consumers and publishers before
// closing the connection
func (conn *Conn) Close(ctx context.Context) error {
	conn.closeConnectionToManagerCh <- struct{}{}
	return conn.connectionManager.Close(ctx)
}

func (conn *Conn) GetNewChannel() (*amqp.Channel, error) {
	con := conn.connectionManager.CheckoutConnection()
	defer conn.connectionManager.CheckinConnection()

	ch, err := con.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func monitorAndWait(stopChan chan struct{}, amqpErrs ...chan *amqp.Error) error {
	result := make(chan error, len(amqpErrs))

	// Setup monitoring for connections and channels can be several connections and several channels.
	// The first one closed will yield the error.
	for _, errCh := range amqpErrs {
		go func(c chan *amqp.Error) {
			err, ok := <-c
			if !ok {
				result <- ErrUnexpectedConnClosed
				return
			}
			result <- err
		}(errCh)
	}

	select {
	case err := <-result:
		return err
	case <-stopChan:
		return nil
	}
}

func createConnections(ctx context.Context, conn *connectionmanager.ConnectionManager) (conn1, conn2 *amqp.Connection, err error) {
	conn1, err = conn.NewConnect(ctx)
	if err != nil {
		return nil, nil, err
	}
	conn2, err = conn.NewConnect(ctx)
	if err != nil {
		return nil, nil, err
	}
	return conn1, conn2, nil
}

func createChannels(inputConn, outputConn *amqp.Connection) (inputCh, outputCh *amqp.Channel, err error) {
	inputCh, err = inputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	outputCh, err = outputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return inputCh, outputCh, nil
}
