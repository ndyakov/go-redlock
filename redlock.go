// Package redlock provides Redis-backed distributed leases using go-redis.
// It supports a single Redis authority and the multi-master Redlock algorithm.
package redlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNotAcquired        = errors.New("redlock: lock not acquired")
	ErrNotHeld            = errors.New("redlock: lock is not held")
	ErrLockLost           = errors.New("redlock: lock ownership lost")
	ErrAlreadyHeld        = errors.New("redlock: lock is already held")
	ErrFencingUnsupported = errors.New("redlock: fencing tokens require single-node mode")
)

const (
	defaultExpiry           = 30 * time.Second
	defaultDelay            = 200 * time.Millisecond
	defaultAttempts         = 3
	defaultDrift            = 0.01
	defaultOperationTimeout = 500 * time.Millisecond
)

const (
	deleteScript        = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
	extendScript        = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`
	fencedAcquireScript = `if redis.call("exists", KEYS[1]) == 0 then local fence = redis.call("incr", KEYS[2]); redis.call("psetex", KEYS[1], ARGV[2], ARGV[1]); return fence else return 0 end`
)

// Node is the go-redis command subset used by this package. *redis.Client,
// *redis.ClusterClient, and *redis.Ring implement Node. A ClusterClient or Ring
// is one logical Redlock node, not one node per shard or replica.
type Node interface {
	SetNX(context.Context, string, interface{}, time.Duration) *redis.BoolCmd
	Eval(context.Context, string, []string, ...interface{}) *redis.Cmd
}

type Mode uint8

const (
	ModeSingle Mode = iota + 1
	ModeRedlock
)

type Config struct {
	Expiry           time.Duration
	RetryDelay       time.Duration
	Attempts         int
	DriftFactor      float64
	OperationTimeout time.Duration
}

type Manager struct {
	nodes  []Node
	config Config
	mode   Mode
}

// New preserves the original API: one node selects single-node mode and three
// or more nodes select Redlock mode. Prefer NewSingle or NewRedlock in new code.
func New(nodes ...Node) (*Manager, error) { return NewWithConfig(Config{}, nodes...) }

func NewWithConfig(cfg Config, nodes ...Node) (*Manager, error) {
	if len(nodes) == 1 {
		return newManager(ModeSingle, cfg, nodes)
	}
	return newManager(ModeRedlock, cfg, nodes)
}

func NewSingle(node Node, cfg ...Config) (*Manager, error) {
	return newManager(ModeSingle, firstConfig(cfg), []Node{node})
}

// NewRedlock requires an odd number of at least three independently operated
// Redis primaries. Replicas of one primary do not satisfy this requirement.
func NewRedlock(nodes []Node, cfg ...Config) (*Manager, error) {
	return newManager(ModeRedlock, firstConfig(cfg), nodes)
}

func firstConfig(configs []Config) Config {
	if len(configs) == 0 {
		return Config{}
	}
	return configs[0]
}

func newManager(mode Mode, cfg Config, nodes []Node) (*Manager, error) {
	if len(nodes) == 0 {
		return nil, errors.New("redlock: at least one Redis node is required")
	}
	if mode == ModeSingle && len(nodes) != 1 {
		return nil, errors.New("redlock: single mode requires exactly one Redis node")
	}
	if mode == ModeRedlock && (len(nodes) < 3 || len(nodes)%2 == 0) {
		return nil, errors.New("redlock: Redlock mode requires an odd number of at least three independent Redis nodes")
	}
	for _, node := range nodes {
		if node == nil {
			return nil, errors.New("redlock: Redis node must not be nil")
		}
	}
	if cfg.Expiry == 0 {
		cfg.Expiry = defaultExpiry
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = defaultDelay
	}
	if cfg.Attempts == 0 {
		cfg.Attempts = defaultAttempts
	}
	if cfg.DriftFactor == 0 {
		cfg.DriftFactor = defaultDrift
	}
	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = cfg.Expiry / 5
		if cfg.OperationTimeout > defaultOperationTimeout {
			cfg.OperationTimeout = defaultOperationTimeout
		}
		if cfg.OperationTimeout < time.Millisecond {
			cfg.OperationTimeout = time.Millisecond
		}
	}
	if cfg.Expiry < time.Millisecond || cfg.RetryDelay < 0 || cfg.Attempts < 1 || cfg.DriftFactor < 0 || cfg.OperationTimeout <= 0 {
		return nil, errors.New("redlock: invalid configuration")
	}
	return &Manager{nodes: append([]Node(nil), nodes...), config: cfg, mode: mode}, nil
}

func (m *Manager) Mode() Mode  { return m.mode }
func (m *Manager) Quorum() int { return len(m.nodes)/2 + 1 }

func (m *Manager) NewLock(key string, options ...LockOption) *Lock {
	l := &Lock{manager: m, key: key, expiry: m.config.Expiry}
	for _, option := range options {
		option(l)
	}
	return l
}

type LockOption func(*Lock)

func WithExpiry(expiry time.Duration) LockOption { return func(lock *Lock) { lock.expiry = expiry } }

// WithAutoRenew renews a held lease at interval until Unlock is called. The
// interval must leave enough time for a quorum renewal before the lease ends.
func WithAutoRenew(interval time.Duration) LockOption {
	return func(lock *Lock) { lock.renewInterval = interval }
}

// WithFencingToken atomically increments counterKey when a single-node lock is
// acquired. The protected resource must reject tokens older than the greatest
// token it has accepted. Fencing is intentionally unavailable in Redlock mode,
// where independent Redis masters cannot produce one linearizable counter.
func WithFencingToken(counterKey string) LockOption {
	return func(lock *Lock) { lock.fenceKey = counterKey }
}

// QuorumError reports an operation that did not complete on the required
// number of nodes. Individual transport errors are retained by node index.
type QuorumError struct {
	Operation  string
	Succeeded  int
	Required   int
	NodeErrors map[int]error
}

func (e *QuorumError) Error() string {
	return fmt.Sprintf("redlock: %s succeeded on %d nodes; %d required (%d node errors)", e.Operation, e.Succeeded, e.Required, len(e.NodeErrors))
}

func (e *QuorumError) Unwrap() error {
	if e.Operation == "acquire" {
		return ErrNotAcquired
	}
	return ErrLockLost
}

type Lock struct {
	manager       *Manager
	key           string
	expiry        time.Duration
	renewInterval time.Duration
	fenceKey      string

	mu         sync.Mutex
	token      string
	fence      uint64
	validUntil time.Time
	lost       chan struct{}
	lostClosed bool
	stopRenew  chan struct{}
}

func (l *Lock) TryLock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.token != "" {
		if time.Now().Before(l.validUntil) {
			return ErrAlreadyHeld
		}
		l.stopRenewalLocked()
		l.markLostLocked()
	}
	if l.key == "" || l.expiry < time.Millisecond {
		return errors.New("redlock: key and expiry of at least one millisecond are required")
	}
	if l.renewInterval < 0 || l.renewInterval >= l.expiry {
		return errors.New("redlock: renewal interval must be positive and shorter than expiry")
	}
	if l.fenceKey != "" && l.manager.mode != ModeSingle {
		return ErrFencingUnsupported
	}

	token, err := randomToken()
	if err != nil {
		return err
	}
	for attempt := 0; attempt < l.manager.config.Attempts; attempt++ {
		started := time.Now()
		results, fence := l.acquireNodes(ctx, token)
		succeeded, nodeErrors := summarize(results)
		validity := l.validity(l.expiry, time.Since(started))
		if succeeded >= l.manager.Quorum() && validity > 0 {
			l.token, l.fence, l.validUntil = token, fence, time.Now().Add(validity)
			l.lost, l.lostClosed = make(chan struct{}), false
			if l.renewInterval > 0 {
				l.startRenewalLocked()
			}
			return nil
		}
		l.cleanup(token)
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt+1 < l.manager.config.Attempts {
			if err := waitRetry(ctx, l.manager.config.RetryDelay); err != nil {
				return err
			}
		}
		if attempt+1 == l.manager.config.Attempts && len(nodeErrors) > 0 {
			return &QuorumError{Operation: "acquire", Succeeded: succeeded, Required: l.manager.Quorum(), NodeErrors: nodeErrors}
		}
	}
	return ErrNotAcquired
}

func (l *Lock) Unlock(ctx context.Context) error {
	l.mu.Lock()
	if l.token == "" {
		l.mu.Unlock()
		return ErrNotHeld
	}
	l.stopRenewalLocked()
	token := l.token
	l.token, l.fence, l.validUntil = "", 0, time.Time{}
	l.mu.Unlock()

	results := l.releaseNodes(ctx, token)
	removed, nodeErrors := summarize(results)
	if len(nodeErrors) > 0 {
		return &QuorumError{Operation: "unlock", Succeeded: len(l.manager.nodes) - len(nodeErrors), Required: len(l.manager.nodes), NodeErrors: nodeErrors}
	}
	if removed == 0 {
		return ErrLockLost
	}
	return nil
}

func (l *Lock) Extend(ctx context.Context, expiry time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.extendLocked(ctx, expiry)
}

func (l *Lock) extendLocked(ctx context.Context, expiry time.Duration) error {
	if l.token == "" {
		return ErrNotHeld
	}
	if expiry < time.Millisecond {
		return errors.New("redlock: expiry must be at least one millisecond")
	}
	started := time.Now()
	results := l.runNodes(ctx, func(ctx context.Context, node Node) (bool, error) {
		result, err := node.Eval(ctx, extendScript, []string{l.key}, l.token, expiry.Milliseconds()).Int64()
		return result == 1, err
	})
	succeeded, nodeErrors := summarize(results)
	validity := l.validity(expiry, time.Since(started))
	if succeeded < l.manager.Quorum() || validity <= 0 {
		token := l.token
		l.markLostLocked()
		go l.cleanup(token)
		if len(nodeErrors) > 0 {
			return &QuorumError{Operation: "extend", Succeeded: succeeded, Required: l.manager.Quorum(), NodeErrors: nodeErrors}
		}
		return ErrLockLost
	}
	l.expiry, l.validUntil = expiry, time.Now().Add(validity)
	return nil
}

func (l *Lock) Held() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.token != "" && time.Now().Before(l.validUntil)
}
func (l *Lock) ValidUntil() time.Time { l.mu.Lock(); defer l.mu.Unlock(); return l.validUntil }
func (l *Lock) FencingToken() uint64  { l.mu.Lock(); defer l.mu.Unlock(); return l.fence }

// Lost returns a channel closed when ownership is lost because renewal fails.
// It is never closed merely because the caller explicitly unlocks.
func (l *Lock) Lost() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost == nil {
		l.lost = make(chan struct{})
	}
	return l.lost
}

func (l *Lock) startRenewalLocked() {
	l.stopRenew = make(chan struct{})
	stop := l.stopRenew
	go func() {
		ticker := time.NewTicker(l.renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), l.manager.config.OperationTimeout)
				l.mu.Lock()
				err := l.extendLocked(ctx, l.expiry)
				l.mu.Unlock()
				cancel()
				if err != nil {
					return
				}
			}
		}
	}()
}

func (l *Lock) stopRenewalLocked() {
	if l.stopRenew != nil {
		close(l.stopRenew)
		l.stopRenew = nil
	}
}

func (l *Lock) markLostLocked() {
	l.token, l.fence, l.validUntil = "", 0, time.Time{}
	if l.lost != nil && !l.lostClosed {
		close(l.lost)
		l.lostClosed = true
	}
}

type nodeResult struct {
	ok    bool
	err   error
	fence uint64
}

func (l *Lock) acquireNodes(ctx context.Context, token string) ([]nodeResult, uint64) {
	if l.fenceKey != "" {
		opCtx, cancel := context.WithTimeout(ctx, l.manager.config.OperationTimeout)
		defer cancel()
		result := make(chan nodeResult, 1)
		go func() {
			value, err := l.manager.nodes[0].Eval(opCtx, fencedAcquireScript, []string{l.key, l.fenceKey}, token, l.expiry.Milliseconds()).Int64()
			fence := uint64(0)
			if value > 0 {
				fence = uint64(value)
			}
			result <- nodeResult{ok: err == nil && value > 0, err: err, fence: fence}
		}()
		select {
		case acquired := <-result:
			return []nodeResult{acquired}, acquired.fence
		case <-opCtx.Done():
			return []nodeResult{{err: opCtx.Err()}}, 0
		}
	}
	return l.runNodes(ctx, func(ctx context.Context, node Node) (bool, error) {
		return node.SetNX(ctx, l.key, token, l.expiry).Result()
	}), 0
}

func (l *Lock) releaseNodes(ctx context.Context, token string) []nodeResult {
	return l.runNodes(ctx, func(ctx context.Context, node Node) (bool, error) {
		result, err := node.Eval(ctx, deleteScript, []string{l.key}, token).Int64()
		return result == 1, err
	})
}

func (l *Lock) cleanup(token string) {
	ctx, cancel := context.WithTimeout(context.Background(), l.manager.config.OperationTimeout)
	defer cancel()
	l.releaseNodes(ctx, token)
}

func (l *Lock) runNodes(parent context.Context, fn func(context.Context, Node) (bool, error)) []nodeResult {
	opCtx, cancel := context.WithTimeout(parent, l.manager.config.OperationTimeout)
	defer cancel()
	results := make(chan struct {
		index  int
		result nodeResult
	}, len(l.manager.nodes))
	for i, node := range l.manager.nodes {
		go func(index int, node Node) {
			ok, err := fn(opCtx, node)
			results <- struct {
				index  int
				result nodeResult
			}{index, nodeResult{ok: ok, err: err}}
		}(i, node)
	}
	out := make([]nodeResult, len(l.manager.nodes))
	received := make([]bool, len(l.manager.nodes))
	for completed := 0; completed < len(l.manager.nodes); completed++ {
		select {
		case result := <-results:
			out[result.index], received[result.index] = result.result, true
		case <-opCtx.Done():
			for i := range out {
				if !received[i] {
					out[i].err = opCtx.Err()
				}
			}
			return out
		}
	}
	return out
}

func summarize(results []nodeResult) (int, map[int]error) {
	succeeded := 0
	failures := make(map[int]error)
	for i, result := range results {
		if result.ok {
			succeeded++
		}
		if result.err != nil {
			failures[i] = result.err
		}
	}
	return succeeded, failures
}

func (l *Lock) validity(expiry, elapsed time.Duration) time.Duration {
	drift := time.Duration(math.Ceil(float64(expiry)*l.manager.config.DriftFactor)) + 2*time.Millisecond
	return expiry - elapsed - drift
}

func waitRetry(ctx context.Context, base time.Duration) error {
	delay := base
	if max := base / 2; max > 0 {
		delay += time.Duration(mathrand.Int63n(int64(max) + 1))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomToken() (string, error) {
	var token [20]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("redlock: generate token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}
