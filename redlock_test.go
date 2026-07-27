package redlock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeNode struct {
	mu       sync.Mutex
	values   map[string]string
	counters map[string]int64
	failSet  bool
	failEval bool
	delay    time.Duration
}

func newFakeNode() *fakeNode {
	return &fakeNode{values: make(map[string]string), counters: make(map[string]int64)}
}

func (n *fakeNode) SetNX(ctx context.Context, key string, value interface{}, _ time.Duration) *redis.BoolCmd {
	if n.delay > 0 {
		select {
		case <-time.After(n.delay):
		case <-ctx.Done():
			return redis.NewBoolResult(false, ctx.Err())
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failSet {
		return redis.NewBoolResult(false, errors.New("unavailable"))
	}
	if _, exists := n.values[key]; exists {
		return redis.NewBoolResult(false, nil)
	}
	n.values[key] = value.(string)
	return redis.NewBoolResult(true, nil)
}

func (n *fakeNode) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if n.delay > 0 {
		select {
		case <-time.After(n.delay):
		case <-ctx.Done():
			return redis.NewCmdResult(nil, ctx.Err())
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failEval {
		return redis.NewCmdResult(nil, errors.New("unavailable"))
	}
	key, token := keys[0], args[0].(string)
	if script == fencedAcquireScript {
		if _, exists := n.values[key]; exists {
			return redis.NewCmdResult(int64(0), nil)
		}
		n.counters[keys[1]]++
		n.values[key] = token
		return redis.NewCmdResult(n.counters[keys[1]], nil)
	}
	if n.values[key] != token {
		return redis.NewCmdResult(int64(0), nil)
	}
	if script == deleteScript {
		delete(n.values, key)
	}
	return redis.NewCmdResult(int64(1), nil)
}

func (n *fakeNode) has(key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.values[key]
	return ok
}

func testManager(t *testing.T, nodes ...Node) *Manager {
	t.Helper()
	m, err := NewWithConfig(Config{Expiry: time.Second, Attempts: 1}, nodes...)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestAcquireAndUnlockQuorum(t *testing.T) {
	n1, n2, n3 := newFakeNode(), newFakeNode(), newFakeNode()
	n3.failSet = true
	lock := testManager(t, n1, n2, n3).NewLock("resource")
	if err := lock.TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !lock.Held() {
		t.Fatal("lock should be held")
	}
	if err := lock.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n1.has("resource") || n2.has("resource") {
		t.Fatal("unlock left keys behind")
	}
}

func TestFailedQuorumCleansPartialLock(t *testing.T) {
	n1, n2, n3 := newFakeNode(), newFakeNode(), newFakeNode()
	n2.failSet, n3.failSet = true, true
	lock := testManager(t, n1, n2, n3).NewLock("resource")
	if err := lock.TryLock(context.Background()); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("got %v", err)
	}
	if n1.has("resource") {
		t.Fatal("partial acquisition was not cleaned up")
	}
}

func TestUnlockDoesNotDeleteAnotherOwnersValue(t *testing.T) {
	n := newFakeNode()
	lock := testManager(t, n).NewLock("resource")
	if err := lock.TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.values["resource"] = "new-owner"
	n.mu.Unlock()
	if err := lock.Unlock(context.Background()); !errors.Is(err, ErrLockLost) {
		t.Fatalf("got %v", err)
	}
	if !n.has("resource") {
		t.Fatal("deleted another owner's lock")
	}
}

func TestExtendRequiresQuorum(t *testing.T) {
	n1, n2, n3 := newFakeNode(), newFakeNode(), newFakeNode()
	lock := testManager(t, n1, n2, n3).NewLock("resource")
	if err := lock.TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	n2.failEval, n3.failEval = true, true
	if err := lock.Extend(context.Background(), 2*time.Second); !errors.Is(err, ErrLockLost) {
		t.Fatalf("got %v", err)
	}
	if lock.Held() {
		t.Fatal("lock should be marked lost")
	}
}

func TestConfigurationValidation(t *testing.T) {
	if _, err := New(); err == nil {
		t.Fatal("expected missing-node error")
	}
	if _, err := NewWithConfig(Config{Attempts: -1}, newFakeNode()); err == nil {
		t.Fatal("expected invalid config error")
	}
	if _, err := NewRedlock([]Node{newFakeNode(), newFakeNode()}); err == nil {
		t.Fatal("expected invalid Redlock topology error")
	}
}

func TestFencingTokensIncrease(t *testing.T) {
	n := newFakeNode()
	m, err := NewSingle(n, Config{Expiry: time.Second, Attempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := m.NewLock("resource", WithFencingToken("resource:fence"))
	if err := first.TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first.FencingToken() != 1 {
		t.Fatalf("first token = %d", first.FencingToken())
	}
	if err := first.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := m.NewLock("resource", WithFencingToken("resource:fence"))
	if err := second.TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if second.FencingToken() != 2 {
		t.Fatalf("second token = %d", second.FencingToken())
	}
}

func TestFencingRejectedForRedlock(t *testing.T) {
	m := testManager(t, newFakeNode(), newFakeNode(), newFakeNode())
	err := m.NewLock("resource", WithFencingToken("fence")).TryLock(context.Background())
	if !errors.Is(err, ErrFencingUnsupported) {
		t.Fatalf("got %v", err)
	}
}

func TestOperationTimeoutBoundsSlowNode(t *testing.T) {
	fast1, fast2, slow := newFakeNode(), newFakeNode(), newFakeNode()
	slow.delay = time.Second
	m, err := NewRedlock([]Node{fast1, fast2, slow}, Config{Expiry: time.Second, Attempts: 1, OperationTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := m.NewLock("resource").TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("acquisition took %v", elapsed)
	}
}

func TestAutoRenewSignalsLoss(t *testing.T) {
	n := newFakeNode()
	m, err := NewSingle(n, Config{Expiry: 100 * time.Millisecond, Attempts: 1, OperationTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	lock := m.NewLock("resource", WithAutoRenew(20*time.Millisecond))
	if err := lock.TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	lost := lock.Lost()
	n.mu.Lock()
	n.failEval = true
	n.mu.Unlock()
	select {
	case <-lost:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("lock loss was not signaled")
	}
	if lock.Held() {
		t.Fatal("lock should not be held after renewal failure")
	}
}

func TestUnlockReportsNodeErrors(t *testing.T) {
	n1, n2, n3 := newFakeNode(), newFakeNode(), newFakeNode()
	lock := testManager(t, n1, n2, n3).NewLock("resource")
	if err := lock.TryLock(context.Background()); err != nil {
		t.Fatal(err)
	}
	n3.mu.Lock()
	n3.failEval = true
	n3.mu.Unlock()
	var quorumErr *QuorumError
	if err := lock.Unlock(context.Background()); !errors.As(err, &quorumErr) {
		t.Fatalf("got %v", err)
	}
}
