package logger

import "context"

// Logger is the interface to send logs to. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	Fatalf(context.Context, string, ...interface{})
	Errorf(context.Context, string, ...interface{})
	Warningf(context.Context, string, ...interface{})
	Infof(context.Context, string, ...interface{})
	Debugf(context.Context, string, ...interface{})
	Noticef(context.Context, string, ...interface{})
}
