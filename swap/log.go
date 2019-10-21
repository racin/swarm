package swap

import (
	log "github.com/ethereum/go-ethereum/log"
)

const (
	// CallDepth is set to 1 in order to influence to reported line number of
	// the log message with 1 skipped stack frame of calling l.Output()
	CallDepth = 1
	//DefaultAction is the default action filter for SwapLogs
	DefaultAction string = "undefined"
)

// Logger wraps the ethereum logger with specific information for swap logging
// this struct contains an action string that is used for grouping similar logs together
// each log contains a context this will be printed on each message
type Logger struct {
	action string
	logger log.Logger
}

func wrapCtx(sl Logger, ctx ...interface{}) []interface{} {
	for _, elem := range ctx {
		if elem == "action" && len(ctx)%2 == 0 {
			return ctx
		}
	}
	ctx = append([]interface{}{"swap_action", sl.action}, ctx...)
	return ctx
}

// Warn is a convenient alias for log.Warn with a defined action context
func (sl Logger) Warn(msg string, ctx ...interface{}) {
	ctx = wrapCtx(sl, ctx...)
	sl.logger.Warn(msg, ctx...)
}

// Error is a convenient alias for log.Error with a defined action context
func (sl Logger) Error(msg string, ctx ...interface{}) {
	ctx = wrapCtx(sl, ctx...)
	sl.logger.Error(msg, ctx...)
}

//Crit is a convenient alias for log.Crit with a defined action context
func (sl Logger) Crit(msg string, ctx ...interface{}) {
	ctx = wrapCtx(sl, ctx...)
	sl.logger.Crit(msg, ctx...)
}

//Info is a convenient alias for log.Info with a defined action context
func (sl Logger) Info(msg string, ctx ...interface{}) {
	ctx = wrapCtx(sl, ctx...)
	sl.logger.Info(msg, ctx...)
}

//Debug is a convenient alias for log.Debug with a defined action context
func (sl Logger) Debug(msg string, ctx ...interface{}) {
	ctx = wrapCtx(sl, ctx...)
	sl.logger.Debug(msg, ctx...)
}

// Trace is a convenient alias for log.Trace with a defined action context
func (sl Logger) Trace(msg string, ctx ...interface{}) {
	ctx = wrapCtx(sl, ctx...)
	sl.logger.Trace(msg, ctx...)
}

// SetLogAction set the current log action prefix
func (sl *Logger) SetLogAction(action string) {
	//Adds action to logger context
	sl.action = action
}

// newLogger return a new SwapLogger Instance with ctx loaded for swap
func newLogger(logPath string, ctx []interface{}) (swapLogger Logger) {
	swapLogger = Logger{
		action: DefaultAction,
	}
	swapLogger.logger = log.New(ctx...)
	setLoggerHandler(logPath, swapLogger.GetLogger())
	return swapLogger
}

// GetLogger returns the underlying logger
func (sl Logger) GetLogger() (logger log.Logger) {
	return sl.logger
}

// GetHandler return the Handler assigned to root
func GetHandler() log.Handler {
	return log.Root().GetHandler()
}
