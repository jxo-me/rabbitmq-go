package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (rpc *RpcClient) startNotifyFlowHandler(ctx context.Context) {
	notifyFlowChan := rpc.chanManager.NotifyFlowSafe(make(chan bool))
	rpc.disablePublishDueToFlowMux.Lock()
	rpc.disablePublishDueToFlow = false
	rpc.disablePublishDueToFlowMux.Unlock()

	for ok := range notifyFlowChan {
		rpc.disablePublishDueToFlowMux.Lock()
		if ok {
			rpc.options.Logger.Warningf(ctx, "pausing publishing due to flow request from server")
			rpc.disablePublishDueToFlow = true
		} else {
			rpc.disablePublishDueToFlow = false
			rpc.options.Logger.Warningf(ctx, "resuming publishing due to flow request from server")
		}
		rpc.disablePublishDueToFlowMux.Unlock()
	}
}

func (rpc *RpcClient) startNotifyBlockedHandler(ctx context.Context) {
	blockings := rpc.connManager.NotifyBlockedSafe(make(chan amqp.Blocking))
	rpc.disablePublishDueToBlockedMux.Lock()
	rpc.disablePublishDueToBlocked = false
	rpc.disablePublishDueToBlockedMux.Unlock()

	for b := range blockings {
		rpc.disablePublishDueToBlockedMux.Lock()
		if b.Active {
			rpc.options.Logger.Warningf(ctx, "pausing publishing due to TCP blocking from server")
			rpc.disablePublishDueToBlocked = true
		} else {
			rpc.disablePublishDueToBlocked = false
			rpc.options.Logger.Warningf(ctx, "resuming publishing due to TCP blocking from server")
		}
		rpc.disablePublishDueToBlockedMux.Unlock()
	}
}
