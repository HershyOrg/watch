package manager

import (
	"sync"

	"github.com/HershyOrg/watch/shared"
)

// VarState holds the state of all watched variables.
// map[varName]RawWatchValue (internal non-generic storage)
type VarState struct {
	mu     sync.RWMutex
	values map[string]shared.RawWatchValue
}

// NewVarState creates a new VarState.
func NewVarState() *VarState {
	return &VarState{
		values: make(map[string]shared.RawWatchValue),
	}
}

// Get retrieves a variable's RawWatchValue.
func (vs *VarState) Get(name string) (shared.RawWatchValue, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	val, ok := vs.values[name]
	return val, ok
}

// Set updates a variable's RawWatchValue.
func (vs *VarState) Set(name string, value shared.RawWatchValue) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.values[name] = value
}

// BatchSet updates multiple variables atomically.
func (vs *VarState) BatchSet(updates map[string]shared.RawWatchValue) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for name, value := range updates {
		vs.values[name] = value
	}
}

// GetAll returns a snapshot of all variable values.
func (vs *VarState) GetAll() map[string]shared.RawWatchValue {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	snapshot := make(map[string]shared.RawWatchValue, len(vs.values))
	for k, v := range vs.values {
		snapshot[k] = v
	}
	return snapshot
}

// Clear removes all variables.
func (vs *VarState) Clear() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.values = make(map[string]shared.RawWatchValue)
}

// ManagerState holds all state managed by the Manager.
type ManagerState struct {
	VarState     *VarState
	ControlState shared.ControlState
	mu           sync.RWMutex
}

// NewManagerState creates a new ManagerState with initial ControlState.
func NewManagerState(initialState shared.ControlState) *ManagerState {
	return &ManagerState{
		VarState:     NewVarState(),
		ControlState: initialState,
	}
}

// GetControlState returns the current ControlState.
func (ms *ManagerState) GetControlState() shared.ControlState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.ControlState
}

// SetControlState updates the ControlState.
func (ms *ManagerState) SetControlState(state shared.ControlState) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.ControlState = state
}

// StateSnapshot returns a complete state snapshot for logging.
type StateSnapshot struct {
	VarState     map[string]shared.RawWatchValue
	ControlState shared.ControlState
}

// Snapshot creates a snapshot of all state.
func (ms *ManagerState) Snapshot() StateSnapshot {
	return StateSnapshot{
		VarState:     ms.VarState.GetAll(),
		ControlState: ms.GetControlState(),
	}
}
