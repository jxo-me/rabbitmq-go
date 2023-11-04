package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (c *RpcClient) startNotifyFlowHandler(ctx context.Context) {
	notifyFlowChan := c.chanManager.NotifyFlowSafe(make(chan bool))
	c.disablePublishDueToFlowMux.Lock()
	c.disablePublishDueToFlow = false
	c.disablePublishDueToFlowMux.Unlock()

	for ok := range notifyFlowChan {
		c.disablePublishDueToFlowMux.Lock()
		if ok {
			c.options.Logger.Warningf(ctx, "pausing publishing due to flow request from server")
			c.disablePublishDueToFlow = true
		} else {
			c.disablePublishDueToFlow = false
			c.options.Logger.Warningf(ctx, "resuming publishing due to flow request from server")
		}
		c.disablePublishDueToFlowMux.Unlock()
	}
}

func (c *RpcClient) startNotifyBlockedHandler(ctx context.Context) {
	blockings := c.connManager.NotifyBlockedSafe(make(chan amqp.Blocking))
	c.disablePublishDueToBlockedMux.Lock()
	c.disablePublishDueToBlocked = false
	c.disablePublishDueToBlockedMux.Unlock()

	for b := range blockings {
		c.disablePublishDueToBlockedMux.Lock()
		if b.Active {
			c.options.Logger.Warningf(ctx, "pausing publishing due to TCP blocking from server")
			c.disablePublishDueToBlocked = true
		} else {
			c.disablePublishDueToBlocked = false
			c.options.Logger.Warningf(ctx, "resuming publishing due to TCP blocking from server")
		}
		c.disablePublishDueToBlockedMux.Unlock()
	}
}
