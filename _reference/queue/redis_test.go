package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// newRedisTestBroker returns a broker on a fresh, uniquely named queue, or
// skips the test when REDIS_ADDR is not set.
func newRedisTestBroker(t *testing.T, lease time.Duration) Broker {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis at %s: %v", addr, err)
	}
	b, err := NewRedisBroker(ctx, rdb, "test-"+newID()[:8], lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		rdb.Del(ctx, b.stream, b.retry, b.dead)
		rdb.Close()
	})
	return b
}
