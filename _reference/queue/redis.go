package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBroker stores the queue in Redis (>= 6.2):
//
//	{name}:stream  main topic, a stream read through a consumer group
//	{name}:retry   sorted set of jobs waiting out their backoff, scored by due time (ms)
//	{name}:dead    dead-letter topic, a stream
//
// The hash tag keeps all keys in one cluster slot so multi-key operations
// stay atomic. A delivery's lease is its idle time in the consumer group's
// pending list; once it exceeds the lease, any consumer may claim it.
type RedisBroker struct {
	rdb    *redis.Client
	stream string
	retry  string
	dead   string
	group  string
	lease  time.Duration
}

const jobField = "job"

// NewRedisBroker creates the main stream and consumer group if needed.
func NewRedisBroker(ctx context.Context, rdb *redis.Client, name string, lease time.Duration) (*RedisBroker, error) {
	b := &RedisBroker{
		rdb:    rdb,
		stream: "{" + name + "}:stream",
		retry:  "{" + name + "}:retry",
		dead:   "{" + name + "}:dead",
		group:  "workers",
		lease:  lease,
	}
	err := rdb.XGroupCreateMkStream(ctx, b.stream, b.group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return nil, fmt.Errorf("queue: create consumer group: %w", err)
	}
	return b, nil
}

func (b *RedisBroker) Enqueue(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return b.rdb.XAdd(ctx, &redis.XAddArgs{Stream: b.stream, Values: map[string]any{jobField: data}}).Err()
}

func (b *RedisBroker) Dequeue(ctx context.Context, consumer string, wait time.Duration) (*Delivery, error) {
	// Take over work abandoned by dead or stuck consumers before new work.
	for {
		msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   b.stream,
			Group:    b.group,
			Consumer: consumer,
			MinIdle:  b.lease,
			Start:    "0-0",
			Count:    1,
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("queue: autoclaim: %w", err)
		}
		if len(msgs) == 0 {
			break
		}
		if d := b.decode(ctx, msgs[0], true); d != nil {
			return d, nil
		}
	}

	block := wait
	if block <= 0 {
		block = -1 // no BLOCK argument: return immediately
	}
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    b.group,
		Consumer: consumer,
		Streams:  []string{b.stream, ">"},
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read: %w", err)
	}
	for _, s := range res {
		for _, m := range s.Messages {
			if d := b.decode(ctx, m, false); d != nil {
				return d, nil
			}
		}
	}
	return nil, nil
}

// decode turns a stream entry into a Delivery. Entries that cannot be decoded
// are removed so they cannot block the queue; they return nil.
func (b *RedisBroker) decode(ctx context.Context, m redis.XMessage, reclaimed bool) *Delivery {
	raw, _ := m.Values[jobField].(string)
	var job Job
	if raw == "" || json.Unmarshal([]byte(raw), &job) != nil {
		_ = b.settle(ctx, m.ID, nil)
		return nil
	}
	return &Delivery{Job: &job, ID: m.ID, Reclaimed: reclaimed}
}

// settle acknowledges and deletes a stream entry, running extra in the same
// MULTI/EXEC transaction.
func (b *RedisBroker) settle(ctx context.Context, id string, extra func(redis.Pipeliner)) error {
	_, err := b.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if extra != nil {
			extra(p)
		}
		p.XAck(ctx, b.stream, b.group, id)
		p.XDel(ctx, b.stream, id)
		return nil
	})
	return err
}

func (b *RedisBroker) Ack(ctx context.Context, d *Delivery) error {
	return b.settle(ctx, d.ID, nil)
}

func (b *RedisBroker) Retry(ctx context.Context, d *Delivery, at time.Time) error {
	data, err := json.Marshal(d.Job)
	if err != nil {
		return err
	}
	return b.settle(ctx, d.ID, func(p redis.Pipeliner) {
		p.ZAdd(ctx, b.retry, redis.Z{Score: float64(at.UnixMilli()), Member: data})
	})
}

func (b *RedisBroker) DeadLetter(ctx context.Context, d *Delivery) error {
	data, err := json.Marshal(d.Job)
	if err != nil {
		return err
	}
	return b.settle(ctx, d.ID, func(p redis.Pipeliner) {
		p.XAdd(ctx, &redis.XAddArgs{Stream: b.dead, Values: map[string]any{jobField: data}})
	})
}

// promoteScript atomically moves due retries onto the main stream, so any
// number of workers can run it concurrently without duplicating jobs.
var promoteScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job in ipairs(due) do
  redis.call('XADD', KEYS[2], '*', 'job', job)
  redis.call('ZREM', KEYS[1], job)
end
return #due
`)

func (b *RedisBroker) PromoteDue(ctx context.Context, now time.Time) (int, error) {
	return promoteScript.Run(ctx, b.rdb, []string{b.retry, b.stream}, now.UnixMilli(), 100).Int()
}

func (b *RedisBroker) ListDead(ctx context.Context, limit int) ([]DeadJob, error) {
	var msgs []redis.XMessage
	var err error
	if limit > 0 {
		msgs, err = b.rdb.XRangeN(ctx, b.dead, "-", "+", int64(limit)).Result()
	} else {
		msgs, err = b.rdb.XRange(ctx, b.dead, "-", "+").Result()
	}
	if err != nil {
		return nil, err
	}
	out := make([]DeadJob, 0, len(msgs))
	for _, m := range msgs {
		raw, _ := m.Values[jobField].(string)
		var job Job
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			return nil, fmt.Errorf("queue: decode dead job %s: %w", m.ID, err)
		}
		out = append(out, DeadJob{ID: m.ID, Job: &job})
	}
	return out, nil
}

// replayScript deletes a dead entry and re-publishes it in one step, so two
// concurrent replays of the same entry enqueue it only once.
var replayScript = redis.NewScript(`
if redis.call('XDEL', KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call('XADD', KEYS[2], '*', 'job', ARGV[2])
return 1
`)

func (b *RedisBroker) Replay(ctx context.Context, deadID string) error {
	msgs, err := b.rdb.XRange(ctx, b.dead, deadID, deadID).Result()
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return ErrNotFound
	}
	raw, _ := msgs[0].Values[jobField].(string)
	var job Job
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return fmt.Errorf("queue: decode dead job %s: %w", deadID, err)
	}
	data, err := json.Marshal(resetForReplay(&job))
	if err != nil {
		return err
	}
	n, err := replayScript.Run(ctx, b.rdb, []string{b.dead, b.stream}, deadID, data).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
