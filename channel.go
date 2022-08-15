package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type channelManager struct {
	logger              Logger
	url                 string
	channel             *amqp.Channel
	connection          *amqp.Connection
	amqpConfig          Config
	channelMux          *sync.RWMutex
	notifyCancelOrClose chan error
	reconnectInterval   time.Duration
	reconnectionCount   uint
}

func newChannelManager(ctx context.Context, url string, conf Config, log Logger, reconnectInterval time.Duration) (*channelManager, error) {
	conn, ch, err := getNewChannel(url, conf)
	if err != nil {
		return nil, err
	}

	chManager := channelManager{
		logger:              log,
		url:                 url,
		connection:          conn,
		channel:             ch,
		channelMux:          &sync.RWMutex{},
		amqpConfig:          conf,
		notifyCancelOrClose: make(chan error),
		reconnectInterval:   reconnectInterval,
	}
	go chManager.startNotifyCancelOrClosed(ctx)
	return &chManager, nil
}

func getNewChannel(url string, conf Config) (*amqp.Connection, *amqp.Channel, error) {
	amqpConn, err := amqp.DialConfig(url, amqp.Config(conf))
	if err != nil {
		return nil, nil, err
	}
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return amqpConn, ch, nil
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (chManager *channelManager) startNotifyCancelOrClosed(ctx context.Context) {
	notifyCloseChan := chManager.channel.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := chManager.channel.NotifyCancel(make(chan string, 1))
	select {
	case err := <-notifyCloseChan:
		if err != nil {
			chManager.logger.Errorf(ctx, "attempting to reconnect to amqp server after close with error: %v", err)
			chManager.reconnectLoop(ctx)
			chManager.logger.Warnf(ctx, "successfully reconnected to amqp server")
			chManager.notifyCancelOrClose <- err
		}
		if err == nil {
			chManager.logger.Infof(ctx, "amqp channel closed gracefully")
		}
	case err := <-notifyCancelChan:
		chManager.logger.Errorf(ctx, "attempting to reconnect to amqp server after cancel with error: %s", err)
		chManager.reconnectLoop(ctx)
		chManager.logger.Warnf(ctx, "successfully reconnected to amqp server after cancel")
		chManager.notifyCancelOrClose <- errors.New(err)
	}
}

// reconnectLoop continuously attempts to reconnect
func (chManager *channelManager) reconnectLoop(ctx context.Context) {
	for {
		chManager.logger.Infof(ctx, "waiting %s seconds to attempt to reconnect to amqp server", chManager.reconnectInterval)
		time.Sleep(chManager.reconnectInterval)
		err := chManager.reconnect(ctx)
		if err != nil {
			chManager.logger.Errorf(ctx, "error reconnecting to amqp server: %v", err)
		} else {
			chManager.reconnectionCount++
			go chManager.startNotifyCancelOrClosed(ctx)
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (chManager *channelManager) reconnect(ctx context.Context) error {
	chManager.channelMux.Lock()
	defer chManager.channelMux.Unlock()
	newConn, newChannel, err := getNewChannel(chManager.url, chManager.amqpConfig)
	if err != nil {
		return err
	}

	err = chManager.channel.Close()
	if err != nil {
		chManager.logger.Warnf(ctx, "error channel Close: %v", err)
	}
	err = chManager.connection.Close()
	if err != nil {
		chManager.logger.Warnf(ctx, "error connection Close: %v", err)
	}

	chManager.connection = newConn
	chManager.channel = newChannel
	return nil
}

// close safely closes the current channel and connection
func (chManager *channelManager) close() error {
	chManager.channelMux.Lock()
	defer chManager.channelMux.Unlock()

	err := chManager.channel.Close()
	if err != nil {
		return err
	}

	err = chManager.connection.Close()
	if err != nil {
		return err
	}
	return nil
}
