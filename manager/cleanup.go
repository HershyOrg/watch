package manager

import "github.com/HershyOrg/watch/shared"

// CleanupBuilder provides a fluent interface for registering cleanup functions.
// It holds a Manager reference and allows optional cleanup registration before Start.
type CleanupBuilder struct {
	manager *Manager
}

// NewCleanupBuilder creates a CleanupBuilder with the given Manager.
func NewCleanupBuilder(mgr *Manager) *CleanupBuilder {
	return &CleanupBuilder{
		manager: mgr,
	}
}

// Cleanup registers a cleanup function to be called on Stop/Kill/Crash.
// It wraps the user's cleanup function in a cleanupAdapter and sets it in the EffectHandler.
// Returns the CleanupBuilder to maintain compatibility with existing code patterns.
func (cb *CleanupBuilder) Cleanup(cleanupFn func(ctx shared.ManageContext)) *CleanupBuilder {
	cleaner := &cleanupAdapter{
		cleanupFn: cleanupFn,
	}

	// Set cleaner in the Manager's MgrFuncRunner
	cb.manager.GetRunner().SetCleaner(cleaner)

	return cb
}

// GetManager returns the underlying Manager reference.
// This allows access to the Manager after cleanup registration.
func (cb *CleanupBuilder) GetManager() *Manager {
	return cb.manager
}

// cleanupAdapter adapts the user's cleanup function to the Cleaner interface.
type cleanupAdapter struct {
	cleanupFn func(ctx shared.ManageContext)
}

// ClearRun implements the Cleaner interface by calling the user's cleanup function.
func (ca *cleanupAdapter) ClearRun(ctx shared.ManageContext) error {
	// Simply call the cleanup function with ManageContext
	ca.cleanupFn(ctx)
	return nil
}
