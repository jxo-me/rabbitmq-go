package rabbitmq

import (
	"context"
	"fmt"
	"github.com/jxo-me/rabbitmq-go/internal/logger"
	"log"
)

// Logger is describes a logging structure. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger logger.Logger

const loggingPrefix = "rabbit"

// stdDebugLogger logs to stdout up to the `DebugF` level
type stdDebugLogger struct{}

func (l stdDebugLogger) Fatalf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s FATAL: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Errorf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s ERROR: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Warningf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s WARN: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Infof(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s INFO: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Debugf(ctx context.Context, format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s DEBUG: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Noticef(ctx context.Context, format string, v ...interface{}) {}
