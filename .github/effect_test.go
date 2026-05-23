package managertest

import (
	"testing"
	"time"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
)

// These tests verify that the Reducer's Reduce() and ReduceDriven() methods
// produce the correct Effects for given events and ControlState.
// The Commander role has been absorbed into the Reducer.

func newTestLogger() *manager.Logger {
	return manager.NewLogger(100)
}

// TestReduce_UserEvent_ProducesRunEffect tests that a UserMessageReceived
// event produces a RunEffect when state is Idle.
func TestReduce_UserEvent_ProducesRunEffect(t *testing.T) {
	state := manager.NewManagerState(shared.ControlIdle)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	event := &manager.UserMessageReceived{
		ReceivedTime: time.Now(),
		UserMessage:  &shared.Message{Content: "test", ReceivedAt: time.Now()},
	}

	effect, triggeredSig := reducer.ReduceSemantic(event)
	if effect == nil {
		t.Fatal("expected effect, got nil")
	}

	runEffect, ok := effect.(*manager.RunEffect)
	if !ok {
		t.Fatalf("expected RunEffect, got %T", effect)
	}
	if runEffect.TriggeredSignal == nil || !runEffect.TriggeredSignal.IsUserSig {
		t.Error("expected TriggeredSignal with IsUserSig=true")
	}
	if triggeredSig == nil || !triggeredSig.IsUserSig {
		t.Error("expected triggeredSig with IsUserSig=true")
	}

	// State should transition to ControlRunDesired
	if state.GetControlState() != shared.ControlRunDesired {
		t.Errorf("expected ControlRunDesired, got %s", state.GetControlState())
	}
}

// TestReduce_VarEvent_ProducesRunEffect tests that a VarSig event
// produces a RunEffect when state is Idle.
func TestReduce_VarEvent_ProducesRunEffect(t *testing.T) {
	t.Skip("VarSig removed, replaced by WM Marker-based integration")
}

// TestReduce_ControlEvent_StopRequested_ProducesCleanupEffect tests
// Idle -> StopDesired produces CleanupEffect with ForState=ControlStopped.
func TestReduce_ControlEvent_StopRequested_ProducesCleanupEffect(t *testing.T) {
	state := manager.NewManagerState(shared.ControlIdle)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	event := &manager.ControlEvent{
		ReceivedTime: time.Now(),
		Kind:         manager.StopRequested,
		Reason:       "user stop",
	}

	effect, _ := reducer.ReduceSemantic(event)
	if effect == nil {
		t.Fatal("expected effect, got nil")
	}

	cleanupEffect, ok := effect.(*manager.CleanupEffect)
	if !ok {
		t.Fatalf("expected CleanupEffect, got %T", effect)
	}
	if cleanupEffect.ForState != shared.ControlStopped {
		t.Errorf("expected ForState=ControlStopped, got %s", cleanupEffect.ForState)
	}
}

// TestReduce_ControlEvent_KillRequested_ProducesCleanupEffect tests
// Idle -> KillDesired produces CleanupEffect with ForState=ControlKilled.
func TestReduce_ControlEvent_KillRequested_ProducesCleanupEffect(t *testing.T) {
	state := manager.NewManagerState(shared.ControlIdle)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	event := &manager.ControlEvent{
		ReceivedTime: time.Now(),
		Kind:         manager.KillRequested,
		Reason:       "user kill",
	}

	effect, _ := reducer.ReduceSemantic(event)
	if effect == nil {
		t.Fatal("expected effect, got nil")
	}

	cleanupEffect, ok := effect.(*manager.CleanupEffect)
	if !ok {
		t.Fatalf("expected CleanupEffect, got %T", effect)
	}
	if cleanupEffect.ForState != shared.ControlKilled {
		t.Errorf("expected ForState=ControlKilled, got %s", cleanupEffect.ForState)
	}
}

// TestReduce_ControlEvent_TerminalState_IgnoresStop tests that
// stop/kill requests are ignored when already in a terminal state.
func TestReduce_ControlEvent_TerminalState_IgnoresStop(t *testing.T) {
	state := manager.NewManagerState(shared.ControlCrashed)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	event := &manager.ControlEvent{
		ReceivedTime: time.Now(),
		Kind:         manager.StopRequested,
		Reason:       "attempt stop from Crashed",
	}

	effect, _ := reducer.ReduceSemantic(event)
	if effect != nil {
		t.Errorf("expected no effect from Crashed state, got %T", effect)
	}
}

// TestReduceDriven_ExecutionCompleted_SettlesIdle tests that
// ExecutionCompleted produces nil effect and settles at ControlIdle.
func TestReduceDriven_ExecutionCompleted_SettlesIdle(t *testing.T) {
	state := manager.NewManagerState(shared.ControlRunDesired)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	effect := reducer.ReduceDriven(&manager.ExecutionCompleted{})
	if effect != nil {
		t.Errorf("expected nil effect, got %T", effect)
	}
	if state.GetControlState() != shared.ControlIdle {
		t.Errorf("expected ControlIdle, got %s", state.GetControlState())
	}
}

// TestReduceDriven_ExecutionFailed_ProducesRecoverEffect tests that
// ExecutionFailed produces RecoverEffect and sets ControlRecoverDesired.
func TestReduceDriven_ExecutionFailed_ProducesRecoverEffect(t *testing.T) {
	state := manager.NewManagerState(shared.ControlRunDesired)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	effect := reducer.ReduceDriven(&manager.ExecutionFailed{Err: nil, Failures: 3})
	if effect == nil {
		t.Fatal("expected effect, got nil")
	}

	if _, ok := effect.(*manager.RecoverEffect); !ok {
		t.Errorf("expected RecoverEffect, got %T", effect)
	}
	if state.GetControlState() != shared.ControlRecoverDesired {
		t.Errorf("expected ControlRecoverDesired, got %s", state.GetControlState())
	}
}

// TestReduceDriven_CleanupCompleted_SettlesTerminal tests that
// CleanupCompleted with ForState=ControlStopped settles at ControlStopped.
func TestReduceDriven_CleanupCompleted_SettlesTerminal(t *testing.T) {
	state := manager.NewManagerState(shared.ControlStopDesired)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	effect := reducer.ReduceDriven(&manager.CleanupCompleted{ForState: shared.ControlStopped})
	if effect != nil {
		t.Errorf("expected nil effect, got %T", effect)
	}
	if state.GetControlState() != shared.ControlStopped {
		t.Errorf("expected ControlStopped, got %s", state.GetControlState())
	}
}

// TestReduceDriven_RecoveryExhausted_SettlesCrashed tests that
// RecoveryExhausted settles at ControlCrashed.
func TestReduceDriven_RecoveryExhausted_SettlesCrashed(t *testing.T) {
	state := manager.NewManagerState(shared.ControlRecoverDesired)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	effect := reducer.ReduceDriven(&manager.RecoveryExhausted{})
	if effect != nil {
		t.Errorf("expected nil effect, got %T", effect)
	}
	if state.GetControlState() != shared.ControlCrashed {
		t.Errorf("expected ControlCrashed, got %s", state.GetControlState())
	}
}

// TestReduceDriven_RecoveryReady_ProducesRunEffect tests that
// RecoveryReady produces RunEffect and sets ControlRunDesired.
func TestReduceDriven_RecoveryReady_ProducesRunEffect(t *testing.T) {
	state := manager.NewManagerState(shared.ControlRecoverDesired)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	effect := reducer.ReduceDriven(&manager.RecoveryReady{})
	if effect == nil {
		t.Fatal("expected effect, got nil")
	}

	if _, ok := effect.(*manager.RunEffect); !ok {
		t.Errorf("expected RunEffect, got %T", effect)
	}
	if state.GetControlState() != shared.ControlRunDesired {
		t.Errorf("expected ControlRunDesired, got %s", state.GetControlState())
	}
}

// TestReduceDriven_ErrorSuppressed_SettlesIdle tests that
// ErrorSuppressed settles at ControlIdle.
func TestReduceDriven_ErrorSuppressed_SettlesIdle(t *testing.T) {
	state := manager.NewManagerState(shared.ControlRunDesired)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	effect := reducer.ReduceDriven(&manager.ErrorSuppressed{})
	if effect != nil {
		t.Errorf("expected nil effect, got %T", effect)
	}
	if state.GetControlState() != shared.ControlIdle {
		t.Errorf("expected ControlIdle, got %s", state.GetControlState())
	}
}

// TestRunEffect_UserMessage_DeliveredToManagedFunc tests that
// a user message flows through RunEffect.Message to the ManagedFunc.
// Replaces TestUserState_Operations: verifies message delivery without shared mutable state.
func TestRunEffect_UserMessage_DeliveredToManagedFunc(t *testing.T) {
	state := manager.NewManagerState(shared.ControlIdle)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	// 1. Reduce: UserMessageReceived -> RunEffect with Message
	msg := &shared.Message{Content: "hello", ReceivedAt: time.Now()}
	event := &manager.UserMessageReceived{
		ReceivedTime: time.Now(),
		UserMessage:  msg,
	}
	effect, triggeredSig := reducer.ReduceSemantic(event)
	if effect == nil {
		t.Fatal("expected effect, got nil")
	}
	runEffect, ok := effect.(*manager.RunEffect)
	if !ok {
		t.Fatalf("expected RunEffect, got %T", effect)
	}

	// RunEffect must carry the message
	if runEffect.Message == nil {
		t.Fatal("expected RunEffect.Message to be non-nil")
	}
	if runEffect.Message.Content != "hello" {
		t.Errorf("expected 'hello', got %s", runEffect.Message.Content)
	}
	if !triggeredSig.IsUserSig {
		t.Error("expected IsUserSig=true")
	}

	// 2. Execute: Target receives message through RunEffect
	var received *shared.Message
	target := manager.NewRunner(
		func(m *shared.Message, ctx shared.ManageContext) error {
			received = m
			return nil
		},
		nil,
		logger,
		shared.DefaultWatcherConfig(),
	)
	target.Execute(runEffect)

	if received == nil {
		t.Fatal("managed function did not receive message")
	}
	if received.Content != "hello" {
		t.Errorf("expected 'hello', got %s", received.Content)
	}
}

// TestRunEffect_NilMessage_VarSigTrigger tests that VarSig-triggered
// RunEffect carries nil Message, and ManagedFunc receives nil.
func TestRunEffect_NilMessage_VarSigTrigger(t *testing.T) {
	t.Skip("VarSig removed, replaced by WM Marker-based integration")
}

// TestRunEffect_ControlTrigger_NilMessage tests that Control-triggered
// RunEffect (e.g. RunRequested from terminal state) carries nil Message.
func TestRunEffect_ControlTrigger_NilMessage(t *testing.T) {
	state := manager.NewManagerState(shared.ControlStopped)
	signals := manager.NewSignalChannels(10)
	logger := newTestLogger()
	reducer := manager.NewReducer(state, signals, logger, nil)

	event := &manager.ControlEvent{
		ReceivedTime: time.Now(),
		Kind:         manager.RunRequested,
		Reason:       "restart",
	}

	effect, _ := reducer.ReduceSemantic(event)
	if effect == nil {
		t.Fatal("expected effect, got nil")
	}
	runEffect, ok := effect.(*manager.RunEffect)
	if !ok {
		t.Fatalf("expected RunEffect, got %T", effect)
	}

	// Control-triggered RunEffect must have nil Message
	if runEffect.Message != nil {
		t.Errorf("expected nil Message for Control trigger, got %v", runEffect.Message)
	}
}
