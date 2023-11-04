package rabbitmq

import (
	"context"
	"fmt"
	"github.com/jxo-me/rabbitmq-go/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sort"
	"strings"
)

// Logger is describing a logging structure. It can be set using
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

func stringifyPublishingForLog(v amqp.Publishing) string {
	vals := []string{}

	if v.CorrelationId != "" {
		vals = append(vals, fmt.Sprintf("CorrelationID=%s", v.CorrelationId))
	}

	if v.Type != "" {
		vals = append(vals, fmt.Sprintf("Type=%s", v.Type))
	}

	if v.AppId != "" {
		vals = append(vals, fmt.Sprintf("AppId=%s", v.AppId))
	}

	if v.UserId != "" {
		vals = append(vals, fmt.Sprintf("UserId=%s", v.UserId))
	}

	if len(v.Headers) > 0 {
		vals = append(vals, fmt.Sprintf("Headers=%s", stringifyTableForLog(v.Headers)))
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyRequestForLog(v *Request) string {
	if v == nil {
		return "[nil]"
	}

	vals := []string{}

	if v.Exchange != "" {
		vals = append(vals, fmt.Sprintf("Exchange=%s", v.Exchange))
	}

	if v.RoutingKey != "" {
		vals = append(vals, fmt.Sprintf("RoutingKey=%s", v.RoutingKey))
	}

	vals = append(vals, fmt.Sprintf("Publishing=%s", stringifyPublishingForLog(v.Publishing)))

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyTableForLog(v amqp.Table) string {
	if len(v) == 0 {
		return "[]"
	}

	vals := []string{}

	for key, val := range v {
		if inner, ok := val.(amqp.Table); ok {
			val = stringifyTableForLog(inner)
		}

		strVal := fmt.Sprintf("%v", val)

		if strVal == "" {
			continue
		}

		vals = append(vals, fmt.Sprintf("%s=%s", key, strVal))
	}

	sort.Strings(vals)

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyDeliveryForLog(v *amqp.Delivery) string {
	if v == nil {
		return "[nil]"
	}

	vals := []string{}

	if v.Exchange != "" {
		vals = append(vals, fmt.Sprintf("Exchange=%s", v.Exchange))
	}

	if v.RoutingKey != "" {
		vals = append(vals, fmt.Sprintf("RoutingKey=%s", v.RoutingKey))
	}

	if v.Type != "" {
		vals = append(vals, fmt.Sprintf("Type=%s", v.Type))
	}

	if v.CorrelationId != "" {
		vals = append(vals, fmt.Sprintf("CorrelationId=%s", v.CorrelationId))
	}

	if v.AppId != "" {
		vals = append(vals, fmt.Sprintf("AppId=%s", v.AppId))
	}

	if v.UserId != "" {
		vals = append(vals, fmt.Sprintf("UserId=%s", v.UserId))
	}

	if len(v.Headers) > 0 {
		vals = append(vals, fmt.Sprintf("Headers=%s", stringifyTableForLog(v.Headers)))
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyReturnForLog(v amqp.Return) string {
	vals := []string{
		fmt.Sprintf("ReplyCode=%d", v.ReplyCode),
		fmt.Sprintf("ReplyText=%s", v.ReplyText),
	}

	if v.Exchange != "" {
		vals = append(vals, fmt.Sprintf("Exchange=%s", v.Exchange))
	}

	if v.RoutingKey != "" {
		vals = append(vals, fmt.Sprintf("RoutingKey=%s", v.RoutingKey))
	}

	if v.CorrelationId != "" {
		vals = append(vals, fmt.Sprintf("CorrelationID=%s", v.CorrelationId))
	}

	if v.Type != "" {
		vals = append(vals, fmt.Sprintf("Type=%s", v.Type))
	}

	if v.AppId != "" {
		vals = append(vals, fmt.Sprintf("AppId=%s", v.AppId))
	}

	if v.UserId != "" {
		vals = append(vals, fmt.Sprintf("UserId=%s", v.UserId))
	}

	if len(v.Headers) > 0 {
		vals = append(vals, fmt.Sprintf("Headers=%s", stringifyTableForLog(v.Headers)))
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}
