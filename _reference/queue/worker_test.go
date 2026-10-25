package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// forEachBroker runs fn against the memory broker and, when REDIS_ADDR is
// set, against Redis.
func forEachBroker(t *testing.T, lease time.Duration, fn func(t *testing.T, b Broker)) {
	t.Run("memory", func(t *testing.T) { fn(t, NewMemoryBroker(lease)) })
	t.Run("redis", func(t *testing.T) { fn(t, newRedisTestBroker(t, lease)) })
}

func testConfig() Config {
	return Config{
		Consumer:        "test-worker",
		Concurrency:     2,
		JobTimeout:      time.Second,
		PollWait:        20 * time.Millisecond,
		PromoteInterval: 5 * time.Millisecond,
		Backoff:         Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond},
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func startWorker(t *testing.T, b Broker, handlers map[string]Handler) {
	t.Helper()
	w := NewWorker(b, testConfig())
	for typ, h := range handlers {
		w.Handle(typ, h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func enqueue(t *testing.T, b Broker, typ string, maxAttempts int) *Job {
	t.Helper()
	job, err := NewJob(typ, map[string]string{"k": "v"}, maxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return job
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func deadJobs(t *testing.T, b Broker) []DeadJob {
	t.Helper()
	dead, err := b.ListDead(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	return dead
}

func TestSuccessIsAcked(t *testing.T) {
	forEachBroker(t, time.Minute, func(t *testing.T, b Broker) {
		var calls atomic.Int32
		startWorker(t, b, map[string]Handler{"ok": func(context.Context, *Job) error {
			calls.Add(1)
			return nil
		}})
		enqueue(t, b, "ok", 3)
		waitFor(t, "handler call", func() bool { return calls.Load() == 1 })
		time.Sleep(100 * time.Millisecond)
		if n := calls.Load(); n != 1 {
			t.Fatalf("handler called %d times, want 1", n)
		}
		if dead := deadJobs(t, b); len(dead) != 0 {
			t.Fatalf("unexpected dead jobs: %+v", dead)
		}
	})
}

func TestTransientFailureIsRetried(t *testing.T) {
	forEachBroker(t, time.Minute, func(t *testing.T, b Broker) {
		var calls, succeeded atomic.Int32
		startWorker(t, b, map[string]Handler{"flaky": func(_ context.Context, j *Job) error {
			if calls.Add(1) <= 2 {
				return errors.New("transient")
			}
			if j.Attempt != 2 || j.LastError != "transient" {
				t.Errorf("third delivery: attempt=%d last_error=%q", j.Attempt, j.LastError)
			}
			succeeded.Add(1)
			return nil
		}})
		enqueue(t, b, "flaky", 5)
		waitFor(t, "success", func() bool { return succeeded.Load() == 1 })
		if dead := deadJobs(t, b); len(dead) != 0 {
			t.Fatalf("unexpected dead jobs: %+v", dead)
		}
	})
}

func TestExhaustedRetriesGoToDeadLetter(t *testing.T) {
	forEachBroker(t, time.Minute, func(t *testing.T, b Broker) {
		var calls atomic.Int32
		startWorker(t, b, map[string]Handler{"broken": func(context.Context, *Job) error {
			calls.Add(1)
			return errors.New("boom")
		}})
		job := enqueue(t, b, "broken", 3)
		waitFor(t, "dead letter", func() bool { return len(deadJobs(t, b)) == 1 })
		dj := deadJobs(t, b)[0]
		if dj.Job.ID != job.ID || dj.Job.Attempt != 3 || dj.Job.LastError != "boom" {
			t.Fatalf("dead job = %+v", dj.Job)
		}
		if n := calls.Load(); n != 3 {
			t.Fatalf("handler called %d times, want 3", n)
		}
	})
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	forEachBroker(t, time.Minute, func(t *testing.T, b Broker) {
		var calls atomic.Int32
		startWorker(t, b, map[string]Handler{"bad-input": func(context.Context, *Job) error {
			calls.Add(1)
			return Permanent(errors.New("invalid payload"))
		}})
		enqueue(t, b, "bad-input", 5)
		waitFor(t, "dead letter", func() bool { return len(deadJobs(t, b)) == 1 })
		if n := calls.Load(); n != 1 {
			t.Fatalf("handler called %d times, want 1", n)
		}
	})
}

func TestPanicIsRetriedThenDeadLettered(t *testing.T) {
	forEachBroker(t, time.Minute, func(t *testing.T, b Broker) {
		startWorker(t, b, map[string]Handler{"panics": func(context.Context, *Job) error {
			panic("nil map")
		}})
		enqueue(t, b, "panics", 2)
		waitFor(t, "dead letter", func() bool { return len(deadJobs(t, b)) == 1 })
		if got := deadJobs(t, b)[0].Job.LastError; !strings.Contains(got, "panic: nil map") {
			t.Fatalf("last error = %q", got)
		}
	})
}

func TestUnknownTypeIsDeadLettered(t *testing.T) {
	forEachBroker(t, time.Minute, func(t *testing.T, b Broker) {
		startWorker(t, b, nil)
		enqueue(t, b, "nobody-handles-this", 5)
		waitFor(t, "dead letter", func() bool { return len(deadJobs(t, b)) == 1 })
		if got := deadJobs(t, b)[0].Job.Attempt; got != 1 {
			t.Fatalf("attempt = %d, want 1", got)
		}
	})
}

func TestAbandonedJobIsReclaimed(t *testing.T) {
	forEachBroker(t, 100*time.Millisecond, func(t *testing.T, b Broker) {
		job := enqueue(t, b, "work", 5)
		// A consumer takes the job and dies without settling it.
		d, err := b.Dequeue(context.Background(), "crashed-worker", time.Second)
		if err != nil || d == nil {
			t.Fatalf("dequeue: %v %v", d, err)
		}

		var seen atomic.Pointer[Job]
		startWorker(t, b, map[string]Handler{"work": func(_ context.Context, j *Job) error {
			seen.Store(j)
			return nil
		}})
		waitFor(t, "reclaimed job to run", func() bool { return seen.Load() != nil })
		got := seen.Load()
		if got.ID != job.ID || got.Attempt != 1 || got.LastError != ErrLeaseExpired.Error() {
			t.Fatalf("reclaimed job = %+v", got)
		}
	})
}

func TestReplayFromDeadLetter(t *testing.T) {
	forEachBroker(t, time.Minute, func(t *testing.T, b Broker) {
		var fixed atomic.Bool
		var succeeded atomic.Int32
		startWorker(t, b, map[string]Handler{"dep": func(_ context.Context, j *Job) error {
			if !fixed.Load() {
				return errors.New("dependency down")
			}
			if j.Attempt != 0 || j.LastError != "" {
				t.Errorf("replayed job not reset: %+v", j)
			}
			succeeded.Add(1)
			return nil
		}})
		enqueue(t, b, "dep", 1)
		waitFor(t, "dead letter", func() bool { return len(deadJobs(t, b)) == 1 })

		fixed.Store(true)
		id := deadJobs(t, b)[0].ID
		if err := b.Replay(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "replayed success", func() bool { return succeeded.Load() == 1 })
		if dead := deadJobs(t, b); len(dead) != 0 {
			t.Fatalf("dead letter not drained: %+v", dead)
		}
		if err := b.Replay(context.Background(), id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("second replay err = %v, want ErrNotFound", err)
		}
	})
}

func TestBackoffDelay(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: time.Second}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w*time.Millisecond {
			t.Errorf("Delay(%d) = %v, want %v", i+1, got, w*time.Millisecond)
		}
	}

	b.Jitter = 0.5
	for i := 0; i < 1000; i++ {
		if got := b.Delay(3); got < 200*time.Millisecond || got > 400*time.Millisecond {
			t.Fatalf("jittered Delay(3) = %v, want within [200ms, 400ms]", got)
		}
	}
}
