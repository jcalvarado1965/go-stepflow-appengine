package stepflow_ae

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/appengine/log"

	stepflow "github.com/jcalvarado1965/go-stepflow"
)

type appengineLogger struct{}

// NewAppengineLogger creates an app engine (stackdriver) logger
func NewAppengineLogger() stepflow.Logger {
	return &appengineLogger{}
}

func (l *appengineLogger) Debugf(ctx context.Context, format string, params ...interface{}) {
	log.Debugf(ctx, "%s", getFormatted(ctx, format, params))
}

func (l *appengineLogger) Infof(ctx context.Context, format string, params ...interface{}) {
	log.Infof(ctx, "%s", getFormatted(ctx, format, params))
}

func (l *appengineLogger) Warnf(ctx context.Context, format string, params ...interface{}) {
	log.Warningf(ctx, "%s", getFormatted(ctx, format, params))
}

func (l *appengineLogger) Errorf(ctx context.Context, format string, params ...interface{}) {
	log.Errorf(ctx, "%s", getFormatted(ctx, format, params))
}

func trimID(id string) string {
	return id[0:strings.IndexAny(id, "-")]
}

func getFormatted(ctx context.Context, format string, params []interface{}) string {
	var runID string
	var flowID string
	var stepID string

	if ctxVal := ctx.Value(stepflow.DataflowRunContextKey); ctxVal != nil {
		runID = fmt.Sprintf("[%s] ", trimID(string(ctxVal.(stepflow.DataflowRunID))))
		if ctxVal = ctx.Value(stepflow.FlowContextKey); ctxVal != nil {
			flowID = fmt.Sprintf("[%s] ", trimID(string(ctxVal.(stepflow.FlowID))))
			if ctxVal = ctx.Value(stepflow.StepContextKey); ctxVal != nil {
				stepID = fmt.Sprintf("[%s] ", ctxVal.(string))
			}
		}
	}
	return runID + flowID + stepID + fmt.Sprintf(format, params...)
}
