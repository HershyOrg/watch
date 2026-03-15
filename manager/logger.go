package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/HershyOrg/watch/shared"
)

// Logger implements both ReduceLogger and EffectLogger interfaces.
type Logger struct {
	mu                      sync.RWMutex
	reduceLog               []ReduceLogEntry
	effectLog               []EffectLogEntry
	effectResults           []*EffectResult
	watchErrorLog           []WatchErrorLogEntry
	contextLog              []ContextValueLogEntry
	stateTransitionFaultLog []StateTransitionFaultLogEntry
	maxEntries              int
}

// ReduceLogEntry represents a single reduce log entry.
type ReduceLogEntry struct {
	LogID     uint64
	Timestamp time.Time
	Action    ReduceAction
}

// EffectLogEntry represents a user log message from effect execution.
type EffectLogEntry struct {
	LogID     uint64
	Timestamp time.Time
	Message   string
}

// WatchErrorPhase represents the phase where a Watch error occurred.
type WatchErrorPhase string

const (
	ErrorPhaseGetComputeFunc     WatchErrorPhase = "get_compute_func"
	ErrorPhaseExecuteComputeFunc WatchErrorPhase = "execute_compute_func"
)

// WatchErrorLogEntry represents a Watch error log entry.
type WatchErrorLogEntry struct {
	LogID      uint64
	Timestamp  time.Time
	VarName    string
	ErrorPhase WatchErrorPhase
	Error      error
}

// ContextValueLogEntry represents a context value change.
type ContextValueLogEntry struct {
	LogID     uint64
	Timestamp time.Time
	Key       string
	OldValue  any
	NewValue  any
	Operation string // "initialized" or "updated"
}

// StateTransitionFaultLogEntry represents a state transition failure.
type StateTransitionFaultLogEntry struct {
	LogID     uint64
	Timestamp time.Time
	FromState shared.ManagerInnerState
	ToState   shared.ManagerInnerState
	Reason    string
	Error     error
}

// NewLogger creates a new Logger with specified max entries per log type.
func NewLogger(maxEntries int) *Logger {
	return &Logger{
		reduceLog:               make([]ReduceLogEntry, 0, maxEntries),
		effectLog:               make([]EffectLogEntry, 0, maxEntries),
		effectResults:           make([]*EffectResult, 0, maxEntries),
		watchErrorLog:           make([]WatchErrorLogEntry, 0, maxEntries),
		contextLog:              make([]ContextValueLogEntry, 0, maxEntries),
		stateTransitionFaultLog: make([]StateTransitionFaultLogEntry, 0, maxEntries),
		maxEntries:              maxEntries,
	}
}

// LogReduce logs a reduce action (implements ReduceLogger).
func (l *Logger) LogReduce(action ReduceAction) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := ReduceLogEntry{
		LogID:     uint64(len(l.reduceLog)) + 1,
		Timestamp: time.Now(),
		Action:    action,
	}

	l.reduceLog = append(l.reduceLog, entry)
	if len(l.reduceLog) >= l.maxEntries {
		// Batch delete: remove first half for better performance
		half := l.maxEntries / 2
		copy(l.reduceLog, l.reduceLog[half:])
		l.reduceLog = l.reduceLog[:half]
	}
}

// LogEffect logs a user message from effect execution (implements EffectLogger).
func (l *Logger) LogEffect(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := EffectLogEntry{
		LogID:     uint64(len(l.effectLog)) + 1,
		Timestamp: time.Now(),
		Message:   msg,
	}

	l.effectLog = append(l.effectLog, entry)
	if len(l.effectLog) >= l.maxEntries {
		// Batch delete: remove first half for better performance
		half := l.maxEntries / 2
		copy(l.effectLog, l.effectLog[half:])
		l.effectLog = l.effectLog[:half]
	}
}

// LogEffectResult logs an effect execution result (implements EffectLogger).
func (l *Logger) LogEffectResult(result *EffectResult) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.effectResults = append(l.effectResults, result)
	if len(l.effectResults) >= l.maxEntries {
		// Batch delete: remove first half for better performance
		half := l.maxEntries / 2
		copy(l.effectResults, l.effectResults[half:])
		l.effectResults = l.effectResults[:half]
	}
}

// GetRecentResults returns the N most recent effect results (implements EffectLogger).
func (l *Logger) GetRecentResults(count int) []*EffectResult {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if count > len(l.effectResults) {
		count = len(l.effectResults)
	}

	results := make([]*EffectResult, count)
	start := len(l.effectResults) - count
	copy(results, l.effectResults[start:])

	return results
}

// LogWatchError logs a Watch error with variable name and phase.
func (l *Logger) LogWatchError(varName string, phase WatchErrorPhase, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := WatchErrorLogEntry{
		LogID:      uint64(len(l.watchErrorLog)) + 1,
		Timestamp:  time.Now(),
		VarName:    varName,
		ErrorPhase: phase,
		Error:      err,
	}

	l.watchErrorLog = append(l.watchErrorLog, entry)
	if len(l.watchErrorLog) >= l.maxEntries {
		// Batch delete: remove first half for better performance
		half := l.maxEntries / 2
		copy(l.watchErrorLog, l.watchErrorLog[half:])
		l.watchErrorLog = l.watchErrorLog[:half]
	}
}

// LogContextValue logs a context value change.
func (l *Logger) LogContextValue(key string, oldValue, newValue any, operation string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := ContextValueLogEntry{
		LogID:     uint64(len(l.contextLog)) + 1,
		Timestamp: time.Now(),
		Key:       key,
		OldValue:  oldValue,
		NewValue:  newValue,
		Operation: operation,
	}

	l.contextLog = append(l.contextLog, entry)
	if len(l.contextLog) >= l.maxEntries {
		// Batch delete: remove first half for better performance
		half := l.maxEntries / 2
		copy(l.contextLog, l.contextLog[half:])
		l.contextLog = l.contextLog[:half]
	}
}

// LogStateTransitionFault logs a state transition failure.
func (l *Logger) LogStateTransitionFault(from, to shared.ManagerInnerState, reason string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := StateTransitionFaultLogEntry{
		LogID:     uint64(len(l.stateTransitionFaultLog)) + 1,
		Timestamp: time.Now(),
		FromState: from,
		ToState:   to,
		Reason:    reason,
		Error:     err,
	}

	l.stateTransitionFaultLog = append(l.stateTransitionFaultLog, entry)
	if len(l.stateTransitionFaultLog) >= l.maxEntries {
		// Batch delete: remove first half for better performance
		half := l.maxEntries / 2
		copy(l.stateTransitionFaultLog, l.stateTransitionFaultLog[half:])
		l.stateTransitionFaultLog = l.stateTransitionFaultLog[:half]
	}
}

// GetReduceLog returns a copy of the reduce log.
func (l *Logger) GetReduceLog() []ReduceLogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logCopy := make([]ReduceLogEntry, len(l.reduceLog))
	copy(logCopy, l.reduceLog)
	return logCopy
}

// GetEffectLog returns a copy of the effect log.
func (l *Logger) GetEffectLog() []EffectLogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logCopy := make([]EffectLogEntry, len(l.effectLog))
	copy(logCopy, l.effectLog)
	return logCopy
}

// GetEffectResults returns a copy of all effect results.
func (l *Logger) GetEffectResults() []EffectResult {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logCopy := make([]EffectResult, len(l.effectResults))
	for i, result := range l.effectResults {
		logCopy[i] = *result
	}
	return logCopy
}


// GetWatchErrorLog returns a copy of the watch error log.
func (l *Logger) GetWatchErrorLog() []WatchErrorLogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logCopy := make([]WatchErrorLogEntry, len(l.watchErrorLog))
	copy(logCopy, l.watchErrorLog)
	return logCopy
}

// GetContextLog returns a copy of the context log.
func (l *Logger) GetContextLog() []ContextValueLogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logCopy := make([]ContextValueLogEntry, len(l.contextLog))
	copy(logCopy, l.contextLog)
	return logCopy
}

// GetStateTransitionFaultLog returns a copy of the state transition fault log.
func (l *Logger) GetStateTransitionFaultLog() []StateTransitionFaultLogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logCopy := make([]StateTransitionFaultLogEntry, len(l.stateTransitionFaultLog))
	copy(logCopy, l.stateTransitionFaultLog)
	return logCopy
}

// PrintSummary prints a summary of all logs.
func (l *Logger) PrintSummary() {
	l.mu.RLock()
	defer l.mu.RUnlock()

	fmt.Printf("\n=== Logger Summary ===\n")
	fmt.Printf("Reduce Log Entries: %d\n", len(l.reduceLog))
	fmt.Printf("Effect Log Entries: %d\n", len(l.effectLog))
	fmt.Printf("Effect Results: %d\n", len(l.effectResults))
	fmt.Printf("Watch Error Log Entries: %d\n", len(l.watchErrorLog))
	fmt.Printf("Context Value Changes: %d\n", len(l.contextLog))
	fmt.Printf("State Transition Fault Entries: %d\n", len(l.stateTransitionFaultLog))

	if len(l.contextLog) > 0 {
		fmt.Printf("\nRecent Context Value Changes:\n")
		start := len(l.contextLog) - 5
		if start < 0 {
			start = 0
		}
		for _, entry := range l.contextLog[start:] {
			fmt.Printf("  [%s] %s: %s (%v → %v)\n",
				entry.Timestamp.Format("15:04:05"),
				entry.Key,
				entry.Operation,
				entry.OldValue,
				entry.NewValue)
		}
	}

	if len(l.watchErrorLog) > 0 {
		fmt.Printf("\nRecent Watch Errors:\n")
		start := len(l.watchErrorLog) - 5
		if start < 0 {
			start = 0
		}
		for _, entry := range l.watchErrorLog[start:] {
			fmt.Printf("  [%s] %s (%s): %v\n",
				entry.Timestamp.Format(time.RFC3339),
				entry.VarName,
				entry.ErrorPhase,
				entry.Error)
		}
	}

	if len(l.stateTransitionFaultLog) > 0 {
		fmt.Printf("\nRecent State Transition Faults:\n")
		start := len(l.stateTransitionFaultLog) - 5
		if start < 0 {
			start = 0
		}
		for _, entry := range l.stateTransitionFaultLog[start:] {
			fmt.Printf("  [%s] %s → %s (reason: %s): %v\n",
				entry.Timestamp.Format(time.RFC3339),
				entry.FromState,
				entry.ToState,
				entry.Reason,
				entry.Error)
		}
	}
}
