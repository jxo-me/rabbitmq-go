package connectionmanager

import (
	"context"
	"sync"
	"time"

	"github.com/jxo-me/rabbitmq-go/internal/dispatcher"
	"github.com/jxo-me/rabbitmq-go/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionManager -
type ConnectionManager struct {
	logger               logger.Logger
	url                  string
	connection           *amqp.Connection
	amqpConfig           amqp.Config
	connectionMux        *sync.RWMutex
	ReconnectInterval    time.Duration
	reconnectionCount    uint
	reconnectionCountMux *sync.Mutex
	dispatcher           *dispatcher.Dispatcher
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(ctx context.Context, url string, conf amqp.Config, log logger.Logger, reconnectInterval time.Duration) (*ConnectionManager, error) {
	conn, err := amqp.DialConfig(url, amqp.Config(conf))
	if err != nil {
		return nil, err
	}
	connManager := ConnectionManager{
		logger:               log,
		url:                  url,
		connection:           conn,
		amqpConfig:           conf,
		connectionMux:        &sync.RWMutex{},
		ReconnectInterval:    reconnectInterval,
		reconnectionCount:    0,
		reconnectionCountMux: &sync.Mutex{},
		dispatcher:           dispatcher.NewDispatcher(),
	}
	go connManager.startNotifyClose(ctx)
	return &connManager, nil
}

// Close safely closes the current channel and connection
func (connManager *ConnectionManager) Close(ctx context.Context) error {
	connManager.logger.Infof(ctx, "closing connection manager...")
	connManager.connectionMux.Lock()
	defer connManager.connectionMux.Unlock()

	err := connManager.connection.Close()
	if err != nil {
		return err
	}
	return nil
}

// NotifyReconnect adds a new subscriber that will receive error messages whenever
// the connection manager has successfully reconnected to the server
func (connManager *ConnectionManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return connManager.dispatcher.AddSubscriber()
}

// CheckoutConnection -
func (connManager *ConnectionManager) CheckoutConnection() *amqp.Connection {
	connManager.connectionMux.RLock()
	return connManager.connection
}

// CheckinConnection -
func (connManager *ConnectionManager) CheckinConnection() {
	connManager.connectionMux.RUnlock()
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (connManager *ConnectionManager) startNotifyClose(ctx context.Context) {
	notifyCloseChan := connManager.connection.NotifyClose(make(chan *amqp.Error, 1))

	err := <-notifyCloseChan
	if err != nil {
		connManager.logger.Errorf(ctx, "attempting to reconnect to amqp server after connection close with error: %v", err)
		connManager.reconnectLoop(ctx)
		connManager.logger.Warningf(ctx, "successfully reconnected to amqp server")
		connManager.dispatcher.Dispatch(err)
	}
	if err == nil {
		connManager.logger.Infof(ctx, "amqp connection closed gracefully")
	}
}

// GetReconnectionCount -
func (connManager *ConnectionManager) GetReconnectionCount() uint {
	connManager.reconnectionCountMux.Lock()
	defer connManager.reconnectionCountMux.Unlock()
	return connManager.reconnectionCount
}

func (connManager *ConnectionManager) incrementReconnectionCount() {
	connManager.reconnectionCountMux.Lock()
	defer connManager.reconnectionCountMux.Unlock()
	connManager.reconnectionCount++
}

// reconnectLoop continuously attempts to reconnect
func (connManager *ConnectionManager) reconnectLoop(ctx context.Context) {
	for {
		connManager.logger.Infof(ctx, "waiting %s seconds to attempt to reconnect to amqp server", connManager.ReconnectInterval)
		time.Sleep(connManager.ReconnectInterval)
		err := connManager.reconnect(ctx)
		if err != nil {
			connManager.logger.Errorf(ctx, "error reconnecting to amqp server: %v", err)
		} else {
			connManager.incrementReconnectionCount()
			go connManager.startNotifyClose(ctx)
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (connManager *ConnectionManager) reconnect(ctx context.Context) error {
	connManager.connectionMux.Lock()
	defer connManager.connectionMux.Unlock()
	newConn, err := amqp.DialConfig(connManager.url, amqp.Config(connManager.amqpConfig))
	if err != nil {
		return err
	}

	if err = connManager.connection.Close(); err != nil {
		connManager.logger.Warningf(ctx, "error closing connection while reconnecting: %v", err)
	}

	connManager.connection = newConn
	return nil
}

// NewConnect safely closes the current channel and obtains a new one
func (connManager *ConnectionManager) NewConnect(ctx context.Context) (*amqp.Connection, error) {
	newConn, err := amqp.DialConfig(connManager.url, connManager.amqpConfig)
	if err != nil {
		connManager.logger.Warningf(ctx, "error new connection : %v", err)
		return nil, err
	}

	return newConn, nil
}
