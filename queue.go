package stepflowae

import (
	"context"
	"encoding/gob"
	"encoding/json"

	stepflow "github.com/jcalvarado1965/go-stepflow"
	"google.golang.org/appengine/delay"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
)

var dequeueCb func(ctx context.Context, flow *stepflow.Flow) error

type appengineQueue struct {
	Logger  stepflow.Logger
	Storage stepflow.Storage
}

var delayFunc = delay.Func("taskFunc", func(ctx context.Context, flow *stepflow.Flow) {
	if dequeueCb != nil {
		// make sure we have not handled this before
		key := "taskFunc:" + string(flow.ID) + ":" + flow.NextStepID
		if count, err := memcache.Increment(ctx, key, 0, 1); count > 1 {
			log.Warningf(ctx, "Received flow more than once: %s", flow.ID)
			return
		} else if err != nil {
			log.Errorf(ctx, "Error incrementing %s", err.Error())
		}
		log.Debugf(ctx, "Handling flow %s, step %s", flow.ID, flow.NextStepID)
		if err := dequeueCb(ctx, flow); err != nil {
			log.Errorf(ctx, "Error handling task: %s", err.Error())
		}
	} else {
		log.Warningf(ctx, "Received task but callback not set!")
	}
})

// NewAppengineQueue creates an appengine queue service
func NewAppengineQueue(logger stepflow.Logger, storage stepflow.Storage) stepflow.FlowQueue {
	mq := &appengineQueue{
		Logger:  logger,
		Storage: storage,
	}
	return mq
}

func (mq *appengineQueue) SetDequeueCb(cb func(ctx context.Context, flow *stepflow.Flow) error) {
	gob.Register(json.RawMessage{})
	dequeueCb = cb
}

func (mq *appengineQueue) Enqueue(ctx context.Context, flow *stepflow.Flow) error {
	mq.Logger.Debugf(ctx, "Enqueueing flow %v", flow)
	if err := delayFunc.Call(ctx, flow); err != nil {
		mq.Logger.Errorf(ctx, "Error enqueueing flow %s", err.Error())
	}
	return nil
}
