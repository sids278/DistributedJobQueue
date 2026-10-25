# DistributedJobQueue

An at-least-once distributed job queue in Go, backed by Redis Streams, with
exponential-backoff retries and a dead-letter topic. Run as many worker
processes as you like, on any machines that can reach Redis.

## How it works

```
 enqueue ──► {jobs}:stream ──XREADGROUP──► worker ──ok──► XACK + XDEL
                 ▲                            │
                 │ promote (Lua, atomic)      ├─ error, attempts left ──► {jobs}:retry  (ZSET, score = due time)
                 └──────── {jobs}:retry ◄─────┘
                                              └─ Permanent(err) or attempts exhausted ──► {jobs}:dead (stream)
```

- **Main topic**: a Redis stream read through one consumer group, so each job
  goes to exactly one worker at a time.
- **Retries**: a failed job is ack'ed off the stream and put in a sorted set
  scored by `now + backoff(attempt)` (exponential, capped, jittered) in the
  same transaction. Every worker runs a Lua script that moves due jobs back
  onto the stream. The script is atomic, so concurrent workers never
  duplicate a job.
- **Dead-letter topic**: a job lands in `{jobs}:dead` when it runs out of
  attempts, when the handler returns `queue.Permanent(err)`, or when no
  handler is registered for its type. The entry keeps `attempt` and
  `last_error`, and you can replay it.
- **Crashed workers**: an un-ack'ed job stays in the consumer group's pending
  list. Once it has been idle longer than the lease, another worker claims it
  with `XAUTOCLAIM`. The claim counts as a failed attempt, so a job that keeps
  crashing workers still ends up dead-lettered instead of looping forever.
- **Shutdown**: on SIGINT/SIGTERM, workers stop taking jobs, finish
  in-flight ones, and settle them before exiting.

Delivery is **at-least-once**, so handlers must be idempotent. Keep
`-job-timeout` below `-lease`, or a slow job is reclaimed and runs twice. The
CLI refuses to start otherwise.

## Usage

```go
w := queue.NewWorker(broker, queue.Config{Concurrency: 8})
w.Handle("send-email", func(ctx context.Context, j *queue.Job) error {
    var p struct{ To string }
    if err := json.Unmarshal(j.Payload, &p); err != nil {
        return queue.Permanent(err) // bad input: don't retry
    }
    return mailer.Send(ctx, p.To)   // error: retried with backoff
})
w.Run(ctx)
```

### CLI

```bash
make redis-up            # or any Redis >= 6.2 on localhost:6379
make build
bin/jobqueue worker -concurrency 4          # start as many as you like
bin/jobqueue enqueue -type flaky -n 10
bin/jobqueue enqueue -type fail -max-attempts 3
bin/jobqueue dlq list
bin/jobqueue dlq replay -all
```

Demo job types: `echo` (succeeds), `flaky` (fails at `payload.fail_rate`,
default 0.5), `fail` (always fails, then dead-lettered), `invalid`
(permanent error).

Global flags: `-redis` (or `REDIS_ADDR`), `-queue` (key prefix, default
`jobs`), `-lease` (default 1m).

## Tests

```bash
make test                                 # in-memory broker only
make test-redis REDIS_ADDR=localhost:6379 # also runs every test against Redis
```

The same behavioural tests run against both `MemoryBroker` and `RedisBroker`:
ack, retry, exhaustion, permanent errors, panics, unknown types, lease
reclaim and DLQ replay.

## Layout

```
queue/job.go      Job, Delivery, Broker interface, Permanent errors
queue/worker.go   worker pool: dispatch, timeouts, panic recovery, retry/DLQ policy
queue/backoff.go  exponential backoff with jitter
queue/redis.go    Redis Streams broker
queue/memory.go   in-process broker for tests and local dev
cmd/jobqueue      CLI: worker, enqueue, dlq list/replay
```
