package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (publisher *Publisher) startNotifyFlowHandler(ctx context.Context) {
	notifyFlowChan := publisher.chManager.channel.NotifyFlow(make(chan bool))
	publisher.disablePublishDueToFlowMux.Lock()
	publisher.disablePublishDueToFlow = false
	publisher.disablePublishDueToFlowMux.Unlock()

	for ok := range notifyFlowChan {
		publisher.disablePublishDueToFlowMux.Lock()
		if ok {
			publisher.options.Logger.Warningf(ctx, "pausing publishing due to flow request from server")
			publisher.disablePublishDueToFlow = true
		} else {
			publisher.disablePublishDueToFlow = false
			publisher.options.Logger.Warningf(ctx, "resuming publishing due to flow request from server")
		}
		publisher.disablePublishDueToFlowMux.Unlock()
	}
}

func (publisher *Publisher) startNotifyBlockedHandler(ctx context.Context) {
	blockings := publisher.chManager.connection.NotifyBlocked(make(chan amqp.Blocking))
	publisher.disablePublishDueToBlockedMux.Lock()
	publisher.disablePublishDueToBlocked = false
	publisher.disablePublishDueToBlockedMux.Unlock()

	for b := range blockings {
		publisher.disablePublishDueToBlockedMux.Lock()
		if b.Active {
			publisher.options.Logger.Warningf(ctx, "pausing publishing due to TCP blocking from server")
			publisher.disablePublishDueToBlocked = true
		} else {
			publisher.disablePublishDueToBlocked = false
			publisher.options.Logger.Warningf(ctx, "resuming publishing due to TCP blocking from server")
		}
		publisher.disablePublishDueToBlockedMux.Unlock()
	}
}
