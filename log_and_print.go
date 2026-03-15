package watch

import (
	"fmt"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
)

// Log writes a message to the effect log via ManageContext.
// This allows users to log from within managed functions.
// The message is logged using the Logger instance associated with the context.
func Log(s string, ctx shared.ManageContext) {
	// Extract the logger from ManageContext
	// ManageContext is implemented by manager.ManageContext which has a logger field
	if mc, ok := ctx.(*manager.ManageContext); ok {
		if logger := getLoggerFromContext(mc); logger != nil {
			logger.LogEffect(s)
		}
	}
}

// PrintWithLog prints a message to stdout and logs it via ManageContext.
// This combines console output with persistent logging.
func PrintWithLog(s string, ctx shared.ManageContext) {
	fmt.Println(s)
	Log(s, ctx)
}

// getLoggerFromContext extracts the logger from ManageContext.
// This is a helper function to access the private logger field.
func getLoggerFromContext(mc *manager.ManageContext) manager.ContextLogger {
	// We need to add a public getter method to ManageContext
	return mc.GetLogger()
}
