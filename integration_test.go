package redlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestIntegrationSingleFencingAndContention(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run Redis integration tests")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	key := fmt.Sprintf("go-redlock:test:%d", time.Now().UnixNano())
	fenceKey := key + ":fence"
	t.Cleanup(func() { client.Del(ctx, key, fenceKey) })
	m, err := NewSingle(client, Config{Expiry: time.Second, Attempts: 1})
	if err != nil {
		t.Fatal(err)
	}

	first := m.NewLock(key, WithFencingToken(fenceKey))
	if err := first.TryLock(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.NewLock(key).TryLock(ctx); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("contender: %v", err)
	}
	firstFence := first.FencingToken()
	if err := first.Unlock(ctx); err != nil {
		t.Fatal(err)
	}

	second := m.NewLock(key, WithFencingToken(fenceKey))
	if err := second.TryLock(ctx); err != nil {
		t.Fatal(err)
	}
	if second.FencingToken() <= firstFence {
		t.Fatalf("fence did not increase: %d <= %d", second.FencingToken(), firstFence)
	}
	if err := second.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationRedlock(t *testing.T) {
	raw := os.Getenv("REDLOCK_REDIS_ADDRS")
	if raw == "" {
		t.Skip("set REDLOCK_REDIS_ADDRS to run Redlock integration tests")
	}
	addresses := strings.Split(raw, ",")
	if len(addresses) < 3 || len(addresses)%2 == 0 {
		t.Fatal("REDLOCK_REDIS_ADDRS must contain an odd number of at least three addresses")
	}
	ctx := context.Background()
	nodes := make([]Node, 0, len(addresses))
	clients := make([]*redis.Client, 0, len(addresses))
	for _, address := range addresses {
		client := redis.NewClient(&redis.Options{Addr: strings.TrimSpace(address)})
		if err := client.Ping(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		clients, nodes = append(clients, client), append(nodes, client)
	}
	t.Cleanup(func() {
		for _, client := range clients {
			client.Close()
		}
	})

	key := fmt.Sprintf("go-redlock:test:%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, client := range clients {
			client.Del(ctx, key)
		}
	})
	m, err := NewRedlock(nodes, Config{Expiry: time.Second, Attempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	lock := m.NewLock(key)
	if err := lock.TryLock(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lock.Extend(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := lock.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
}
