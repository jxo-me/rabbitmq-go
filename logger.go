package rabbitmq

import (
	"context"
	"fmt"
	"log"
)

// Logger is the interface to send logs to. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	Fatalf(context.Context, string, ...interface{})
	Errorf(context.Context, string, ...interface{})
	Warnf(context.Context, string, ...interface{})
	Infof(context.Context, string, ...interface{})
	Debugf(context.Context, string, ...interface{})
	Tracef(context.Context, string, ...interface{})
}

const loggingPrefix = "rabbit"

// stdDebugLogger logs to stdout up to the `DebugF` level
type stdDebugLogger struct{}

func (l stdDebugLogger) Fatalf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s FATAL: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Errorf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s ERROR: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Warnf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s WARN: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Infof(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s INFO: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Debugf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s DEBUG: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Tracef(ctx context.Context, format string, v ...interface{}) {}
