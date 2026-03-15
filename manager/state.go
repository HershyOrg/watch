package manager

import (
	"sync"

	"github.com/HershyOrg/watch/shared"
)

// VarState holds the state of all watched variables.
// map[varName]RawHershValue (internal non-generic storage)
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

// Get retrieves a variable's RawHershValue.
func (vs *VarState) Get(name string) (shared.RawWatchValue, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	val, ok := vs.values[name]
	return val, ok
}

// Set updates a variable's RawHershValue.
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

// UserState holds the current user message state.
type UserState struct {
	mu      sync.RWMutex
	message *shared.Message
}

// NewUserState creates a new UserState.
func NewUserState() *UserState {
	return &UserState{}
}

// GetMessage retrieves the current message.
func (us *UserState) GetMessage() *shared.Message {
	us.mu.RLock()
	defer us.mu.RUnlock()
	return us.message
}

// SetMessage updates the current message.
func (us *UserState) SetMessage(msg *shared.Message) {
	us.mu.Lock()
	defer us.mu.Unlock()
	us.message = msg
}

// ConsumeMessage marks the message as consumed and returns it.
func (us *UserState) ConsumeMessage() *shared.Message {
	us.mu.Lock()
	defer us.mu.Unlock()
	if us.message != nil {
		us.message.IsConsumed = true
		msg := us.message
		us.message = nil
		return msg
	}
	return nil
}

// ManagerState holds all state managed by the Manager.
type ManagerState struct {
	VarState          *VarState
	UserState         *UserState
	ManagerInnerState shared.ManagerInnerState
	mu                sync.RWMutex
}

// NewManagerState creates a new ManagerState with initial ManagerInnerState.
func NewManagerState(initialState shared.ManagerInnerState) *ManagerState {
	return &ManagerState{
		VarState:          NewVarState(),
		UserState:         NewUserState(),
		ManagerInnerState: initialState,
	}
}

// GetManagerInnerState returns the current ManagerInnerState.
func (ms *ManagerState) GetManagerInnerState() shared.ManagerInnerState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.ManagerInnerState
}

// SetManagerInnerState updates the ManagerInnerState.
func (ms *ManagerState) SetManagerInnerState(state shared.ManagerInnerState) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.ManagerInnerState = state
}

// Snapshot returns a complete state snapshot for logging.
type StateSnapshot struct {
	VarState          map[string]shared.RawWatchValue
	UserMessage       *shared.Message
	ManagerInnerState shared.ManagerInnerState
}

// Snapshot creates a snapshot of all state.
func (ms *ManagerState) Snapshot() StateSnapshot {
	return StateSnapshot{
		VarState:          ms.VarState.GetAll(),
		UserMessage:       ms.UserState.GetMessage(),
		ManagerInnerState: ms.GetManagerInnerState(),
	}
}
