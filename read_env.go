package watch

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/HershyOrg/watch/shared"
)

const UserEnvFileName = ".watch.env"

var (
	ErrUserEnvCallerUnknown = errors.New("watch user env caller unknown")
	ErrUserEnvInvalidKey    = errors.New("watch user env key invalid")
	ErrUserEnvFileMissing   = errors.New("watch user env file missing")
	ErrUserEnvMalformed     = errors.New("watch user env file malformed")
	ErrUserEnvDuplicateKey  = errors.New("watch user env duplicate key")
	ErrUserEnvMissingKey    = errors.New("watch user env key missing")
)

type userEnvFileCache struct {
	once   sync.Once
	values map[string]string
	err    error
}

var userEnvCaches sync.Map // map[string]*userEnvFileCache

// ReadEnv reads a user-provided string from the caller directory's .watch.env.
// It never reads process environment variables and caches each .watch.env file
// by absolute path after the first parse. Read failures panic with
// WatchInitPanic because .watch.env is initialization input.
func ReadEnv(key string) string {
	if !validUserEnvKey(key) {
		panicReadEnv(key, fmt.Errorf("%w: %q", ErrUserEnvInvalidKey, key))
	}
	_, callerFile, _, ok := runtime.Caller(1)
	if !ok || callerFile == "" {
		panicReadEnv(key, ErrUserEnvCallerUnknown)
	}
	path, err := filepath.Abs(filepath.Join(filepath.Dir(callerFile), UserEnvFileName))
	if err != nil {
		panicReadEnv(key, fmt.Errorf("resolve %s: %w", UserEnvFileName, err))
	}
	value, err := readUserEnv(path, key)
	if err != nil {
		panicReadEnv(key, err)
	}
	return value
}

func panicReadEnv(key string, cause error) {
	panic(shared.NewWatchInitPanic("ReadEnv:"+key, "failed to read user env", cause))
}

func readUserEnv(path, key string) (string, error) {
	actual, _ := userEnvCaches.LoadOrStore(path, &userEnvFileCache{})
	cache := actual.(*userEnvFileCache)
	cache.once.Do(func() {
		cache.values, cache.err = parseUserEnvFile(path)
	})
	if cache.err != nil {
		return "", cache.err
	}
	value, ok := cache.values[key]
	if !ok {
		return "", fmt.Errorf("%w: %q in %s", ErrUserEnvMissingKey, key, path)
	}
	return value, nil
}

func parseUserEnvFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrUserEnvFileMissing, path)
		}
		return nil, err
	}
	defer file.Close()

	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSuffix(scanner.Text(), "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			return nil, fmt.Errorf("%w: %s:%d missing '='", ErrUserEnvMalformed, path, lineNo)
		}
		key := strings.TrimSpace(line[:idx])
		if !validUserEnvKey(key) {
			return nil, fmt.Errorf("%w: %s:%d invalid key %q", ErrUserEnvMalformed, path, lineNo, key)
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("%w: %s:%d key %q", ErrUserEnvDuplicateKey, path, lineNo, key)
		}
		values[key] = line[idx+1:]
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func validUserEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		if r == '_' || ('A' <= r && r <= 'Z') || ('a' <= r && r <= 'z') || (i > 0 && '0' <= r && r <= '9') {
			continue
		}
		return false
	}
	return true
}
