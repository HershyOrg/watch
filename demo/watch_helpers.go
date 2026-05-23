package main

import (
	"fmt"

	"github.com/HershyOrg/watch"
)

func watcherStateString(watcher *watch.Watcher) string {
	state, err := watcher.State()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return state.String()
}

func printWatcherSummary(watcher *watch.Watcher) {
	logger, err := watcher.Logger()
	if err != nil {
		fmt.Printf("Error getting logger: %v\n", err)
		return
	}
	logger.PrintSummary()
}
