// Package manager provides the core reactive execution engine.
package manager

import (
	"context"
	"sync"

	"github.com/HershyOrg/watch/shared"
)

// ContextLogger interface for context value logging and effect logging.
type ContextLogger interface {
	LogContextValue(key string, oldValue, newValue any, operation string)
	LogEffect(msg string)
}

// contextEntry is a single entry in the unified value store.
// frozen entries (injected via SetFrozenValues) cannot be overwritten by SetValue/UpdateValue.
type contextEntry struct {
	value  any
	frozen bool
}

// ManageContext implements shared.ManageContext interface.
// This is a concrete implementation that manages execution context,
// messages, manager reference, and user-defined values.
type ManageContext struct {
	context.Context
	message         *shared.Message
	triggeredSignal *shared.TriggeredSignal
	manager         *Manager // Manager reference (type-safe!)
	valueStore      map[string]contextEntry
	valuesMutex     sync.RWMutex
	logger          ContextLogger
}

// NewManageContext creates a new ManageContext with the given parameters.
func NewManageContext(ctx context.Context, logger ContextLogger) *ManageContext {
	return &ManageContext{
		Context:    ctx,
		message:    nil,
		manager:    nil,
		valueStore: make(map[string]contextEntry),
		logger:     logger,
	}
}

// GetManager returns the Manager reference (type-safe!).
func (mc *ManageContext) GetManager() *Manager {
	return mc.manager
}

// SetManager sets the manager reference.
func (mc *ManageContext) SetManager(manager *Manager) {
	mc.manager = manager
}

func (mc *ManageContext) Message() *shared.Message {
	return mc.message
}

func (mc *ManageContext) GetTriggeredSignal() *shared.TriggeredSignal {
	mc.valuesMutex.RLock()
	defer mc.valuesMutex.RUnlock()
	return mc.triggeredSignal
}

func (mc *ManageContext) GetValue(key string) any {
	mc.valuesMutex.RLock()
	defer mc.valuesMutex.RUnlock()
	entry, ok := mc.valueStore[key]
	if !ok {
		return nil
	}
	return entry.value
}

func (mc *ManageContext) SetValue(key string, value any) {
	mc.valuesMutex.Lock()
	defer mc.valuesMutex.Unlock()

	if entry, exists := mc.valueStore[key]; exists && entry.frozen {
		return
	}

	oldEntry := mc.valueStore[key]
	mc.valueStore[key] = contextEntry{value: value}

	if mc.logger != nil {
		if oldEntry.value == nil {
			mc.logger.LogContextValue(key, nil, value, "initialized")
		} else {
			mc.logger.LogContextValue(key, oldEntry.value, value, "updated")
		}
	}
}

func (mc *ManageContext) UpdateValue(key string, updateFn func(current any) any) any {
	mc.valuesMutex.Lock()
	defer mc.valuesMutex.Unlock()

	entry := mc.valueStore[key]
	if entry.frozen {
		return entry.value
	}

	currentCopy := shared.DeepCopy(entry.value)
	newValue := updateFn(currentCopy)

	oldValue := entry.value
	mc.valueStore[key] = contextEntry{value: newValue}

	if mc.logger != nil {
		if oldValue == nil {
			mc.logger.LogContextValue(key, nil, newValue, "initialized")
		} else {
			mc.logger.LogContextValue(key, oldValue, newValue, "updated")
		}
	}

	return newValue
}

// SetMessage updates the current message.
// This is called internally by the framework during execution.
func (mc *ManageContext) SetMessage(msg *shared.Message) {
	mc.message = msg
}

// SetTriggeredSignal updates the triggered signal information.
// This is called internally by the framework during execution.
func (mc *ManageContext) SetTriggeredSignal(ts *shared.TriggeredSignal) {
	mc.valuesMutex.Lock()
	defer mc.valuesMutex.Unlock()
	mc.triggeredSignal = ts
}

// UpdateContext replaces the underlying context.
// This is used by EffectHandler when creating execution contexts with timeouts.
func (mc *ManageContext) UpdateContext(ctx context.Context) {
	mc.Context = ctx
}

// SetFrozenValues injects immutable values into the store.
// Frozen entries cannot be overwritten by SetValue/UpdateValue.
// This should only be called during initialization (by Manager creation).
func (mc *ManageContext) SetFrozenValues(values map[string]any) {
	mc.valuesMutex.Lock()
	defer mc.valuesMutex.Unlock()

	for k, v := range values {
		mc.valueStore[k] = contextEntry{value: v, frozen: true}
		if mc.logger != nil {
			mc.logger.LogContextValue(k, nil, v, "frozen_initialized")
		}
	}
}

// GetLogger returns the logger instance.
func (mc *ManageContext) GetLogger() ContextLogger {
	return mc.logger
}

// GetWatcher returns the manager as any to implement shared.ManageContext interface.
// This maintains compatibility with the interface while internally using Manager.
func (mc *ManageContext) GetWatcher() any {
	return mc.manager
}

// GetMachineRegistry returns the MachineRegistry as any to implement shared.ManageContext interface.
func (mc *ManageContext) GetMachineRegistry() any {
	if mc.manager == nil {
		return nil
	}
	return mc.manager.GetMachineRegistry()
}