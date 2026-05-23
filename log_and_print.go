package watch

import (
	"errors"
	"fmt"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
)

var ErrInvalidLogContext = errors.New("invalid log context")

// Log writes a message to the effect log via ManageContext.
// This allows users to log from within managed functions.
// The message is logged using the Logger instance associated with the context.
func Log(s string, ctx shared.ManageContext) error {
	// Extract the logger from ManageContext
	// ManageContext is implemented by manager.ManageContext which has a logger field
	mc, ok := ctx.(*manager.ManageContext)
	if !ok || mc == nil {
		return fmt.Errorf("%w: expected *manager.ManageContext, got %T", ErrInvalidLogContext, ctx)
	}
	logger := getLoggerFromContext(mc)
	if logger == nil {
		return fmt.Errorf("%w: logger is nil", ErrInvalidLogContext)
	}
	logger.LogEffect(s)
	return nil
}

// PrintWithLog prints a message to stdout and logs it via ManageContext.
// This combines console output with persistent logging.
func PrintWithLog(s string, ctx shared.ManageContext) error {
	fmt.Println(s)
	return Log(s, ctx)
}

// getLoggerFromContext extracts the logger from ManageContext.
// This is a helper function to access the private logger field.
func getLoggerFromContext(mc *manager.ManageContext) manager.ContextLogger {
	// We need to add a public getter method to ManageContext
	return mc.GetLogger()
}
