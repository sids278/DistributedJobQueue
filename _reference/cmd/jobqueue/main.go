// Command jobqueue runs workers, enqueues jobs, and inspects or replays the
// dead-letter topic of a Redis-backed queue.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"jobqueue/queue"
)

const usage = `usage: jobqueue [global flags] <command> [flags]

commands:
  worker                      process jobs until SIGINT/SIGTERM
  enqueue -type T [-payload JSON] [-max-attempts N] [-n COUNT]
  dlq list [-limit N]         show dead-lettered jobs
  dlq replay ID... | -all     move dead-lettered jobs back onto the queue

global flags:
`

func main() {
	global := flag.NewFlagSet("jobqueue", flag.ExitOnError)
	addr := global.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address (env REDIS_ADDR)")
	name := global.String("queue", "jobs", "queue name")
	lease := global.Duration("lease", time.Minute, "how long a worker may hold a job before others reclaim it")
	global.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		global.PrintDefaults()
	}
	_ = global.Parse(os.Args[1:])
	args := global.Args()
	if len(args) == 0 {
		global.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *addr})
	defer rdb.Close()
	broker, err := queue.NewRedisBroker(ctx, rdb, *name, *lease)
	if err != nil {
		fatal(err)
	}

	switch args[0] {
	case "worker":
		err = runWorker(ctx, broker, *lease, args[1:])
	case "enqueue":
		err = runEnqueue(ctx, broker, args[1:])
	case "dlq":
		err = runDLQ(ctx, broker, args[1:])
	default:
		global.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

func runWorker(ctx context.Context, b queue.Broker, lease time.Duration, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	concurrency := fs.Int("concurrency", 4, "jobs processed in parallel")
	jobTimeout := fs.Duration("job-timeout", 30*time.Second, "max time per job; must be below -lease")
	_ = fs.Parse(args)
	if *jobTimeout >= lease {
		return fmt.Errorf("-job-timeout (%s) must be below -lease (%s), or slow jobs run twice", *jobTimeout, lease)
	}

	w := queue.NewWorker(b, queue.Config{Concurrency: *concurrency, JobTimeout: *jobTimeout})
	registerDemoHandlers(w)
	slog.Info("worker started", "concurrency", *concurrency)
	err := w.Run(ctx)
	slog.Info("worker stopped")
	return err
}

// registerDemoHandlers wires example job types. Replace with real handlers.
func registerDemoHandlers(w *queue.Worker) {
	// echo always succeeds.
	w.Handle("echo", func(_ context.Context, j *queue.Job) error {
		slog.Info("echo", "job_id", j.ID, "payload", string(j.Payload))
		return nil
	})
	// flaky fails with probability payload.fail_rate (default 0.5).
	w.Handle("flaky", func(_ context.Context, j *queue.Job) error {
		p := struct {
			FailRate *float64 `json:"fail_rate"`
		}{}
		if len(j.Payload) > 0 {
			if err := json.Unmarshal(j.Payload, &p); err != nil {
				return queue.Permanent(fmt.Errorf("bad payload: %w", err))
			}
		}
		rate := 0.5
		if p.FailRate != nil {
			rate = *p.FailRate
		}
		if rand.Float64() < rate {
			return errors.New("simulated transient failure")
		}
		return nil
	})
	// fail always fails, so it ends up on the dead-letter topic after retries.
	w.Handle("fail", func(context.Context, *queue.Job) error {
		return errors.New("simulated failure")
	})
	// invalid fails permanently and is dead-lettered without retries.
	w.Handle("invalid", func(context.Context, *queue.Job) error {
		return queue.Permanent(errors.New("simulated validation error"))
	})
}

func runEnqueue(ctx context.Context, b queue.Broker, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	typ := fs.String("type", "", "job type (required)")
	payload := fs.String("payload", "", "JSON payload")
	maxAttempts := fs.Int("max-attempts", queue.DefaultMaxAttempts, "attempts before dead-lettering")
	n := fs.Int("n", 1, "number of copies to enqueue")
	_ = fs.Parse(args)
	if *typ == "" {
		return errors.New("enqueue: -type is required")
	}
	var raw json.RawMessage
	if *payload != "" {
		if !json.Valid([]byte(*payload)) {
			return errors.New("enqueue: -payload is not valid JSON")
		}
		raw = json.RawMessage(*payload)
	}
	for i := 0; i < *n; i++ {
		job, err := queue.NewJob(*typ, raw, *maxAttempts)
		if err != nil {
			return err
		}
		if err := b.Enqueue(ctx, job); err != nil {
			return err
		}
		fmt.Println(job.ID)
	}
	return nil
}

func runDLQ(ctx context.Context, b queue.Broker, args []string) error {
	if len(args) == 0 {
		return errors.New("dlq: expected 'list' or 'replay'")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("dlq list", flag.ExitOnError)
		limit := fs.Int("limit", 50, "max entries (0 = all)")
		_ = fs.Parse(args[1:])
		dead, err := b.ListDead(ctx, *limit)
		if err != nil {
			return err
		}
		for _, d := range dead {
			fmt.Printf("%s\t%s\t%s\tattempts=%d\t%s\n", d.ID, d.Job.ID, d.Job.Type, d.Job.Attempt, d.Job.LastError)
		}
		return nil
	case "replay":
		fs := flag.NewFlagSet("dlq replay", flag.ExitOnError)
		all := fs.Bool("all", false, "replay every dead-lettered job")
		_ = fs.Parse(args[1:])
		ids := fs.Args()
		if *all {
			dead, err := b.ListDead(ctx, 0)
			if err != nil {
				return err
			}
			for _, d := range dead {
				ids = append(ids, d.ID)
			}
		}
		if len(ids) == 0 {
			return errors.New("dlq replay: give dead-letter IDs or -all")
		}
		for _, id := range ids {
			if err := b.Replay(ctx, id); err != nil {
				return fmt.Errorf("replay %s: %w", id, err)
			}
			fmt.Println("replayed", id)
		}
		return nil
	default:
		return fmt.Errorf("dlq: unknown subcommand %q", args[0])
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "jobqueue:", err)
	os.Exit(1)
}
