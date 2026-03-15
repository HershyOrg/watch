package watch

import "fmt"

// Memo caches a computed value for the duration of the Watcher session.
// On first call, it computes and caches the value.
// On subsequent calls, it returns the cached value.
//
// Memo is synchronous and does NOT trigger re-execution.
// It's useful for expensive initialization that should happen once.
//
// The function is generic and type-safe - it returns the same type T
// as the computeValue function produces.
//
// Example:
//
//	client := watch.Memo(func() *Client {
//	    return expensive.NewClient()
//	}, "apiClient", ctx)
func Memo[T any](computeValue func() T, memoName string, ctx ManageContext) T {
	mgr := getManagerFromContext(ctx)
	if mgr == nil {
		panic("Memo called with invalid ManageContext")
	}

	memoCache := mgr.GetMemoCache()

	cached, exists := memoCache.Load(memoName)
	if exists {
		// Type assert to T - this is safe because we only store T values
		if typedValue, ok := cached.(T); ok {
			return typedValue
		}
		// If type assertion fails, it means the cache was corrupted or
		// the same key was used with different types
		var zero T
		panic(fmt.Sprintf("Memo[%s]: type mismatch - cached type %T, expected %T",
			memoName, cached, zero))
	}

	// Compute value
	value := computeValue()

	// Cache it with size limit check
	err := mgr.SetMemo(memoName, value)
	if err != nil {
		// Cache limit reached, but we still have the computed value
		// Log warning but return the value
		if logger := mgr.GetLogger(); logger != nil {
			logger.LogEffect(fmt.Sprintf("Warning: %v", err))
		}
		return value
	}

	// Log the memoization (only if we stored it)
	if logger := mgr.GetLogger(); logger != nil {
		logger.LogEffect(fmt.Sprintf("Memo[%s] = %v", memoName, value))
	}

	return value
}

// ClearMemo removes a memoized value, forcing recomputation on next Memo call.
func ClearMemo(memoName string, ctx ManageContext) {
	mgr := getManagerFromContext(ctx)
	if mgr == nil {
		panic("ClearMemo called with invalid ManageContext")
	}

	memoCache := mgr.GetMemoCache()
	memoCache.Delete(memoName)

	if logger := mgr.GetLogger(); logger != nil {
		logger.LogEffect(fmt.Sprintf("Memo[%s] cleared", memoName))
	}
}
