package queue

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Handler processes one job. Return nil to acknowledge it, an error to retry
// it with backoff, or Permanent(err) to dead-letter it immediately. Delivery
// is at-least-once, so handlers must be idempotent.
type Handler func(ctx context.Context, job *Job) error

// Config tunes a Worker. Zero values take the defaults noted.
type Config struct {
	// Consumer identifies this worker to the broker. Must be unique per
	// process. Default: hostname-pid.
	Consumer string
	// Concurrency is the number of jobs processed in parallel. Default 4.
	Concurrency int
	// JobTimeout bounds one handler call. Keep it below the broker lease,
	// or a slow job is reclaimed and runs twice. Default 30s.
	JobTimeout time.Duration
	// PollWait is how long one Dequeue blocks. Default 2s.
	PollWait time.Duration
	// PromoteInterval is how often due retries are moved back onto the main
	// topic. Default 500ms.
	PromoteInterval time.Duration
	// Backoff schedules retries. Default DefaultBackoff.
	Backoff Backoff
	Logger  *slog.Logger
}

func (c *Config) setDefaults() {
	if c.Consumer == "" {
		host, _ := os.Hostname()
		c.Consumer = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = 30 * time.Second
	}
	if c.PollWait <= 0 {
		c.PollWait = 2 * time.Second
	}
	if c.PromoteInterval <= 0 {
		c.PromoteInterval = 500 * time.Millisecond
	}
	if c.Backoff == (Backoff{}) {
		c.Backoff = DefaultBackoff
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Worker pulls jobs from a Broker and dispatches them to handlers by type.
type Worker struct {
	broker   Broker
	cfg      Config
	handlers map[string]Handler
}

func NewWorker(b Broker, cfg Config) *Worker {
	cfg.setDefaults()
	return &Worker{broker: b, cfg: cfg, handlers: map[string]Handler{}}
}

// Handle registers h for jobs of the given type. Call before Run.
func (w *Worker) Handle(jobType string, h Handler) {
	w.handlers[jobType] = h
}

// Run processes jobs until ctx is cancelled, then waits for in-flight jobs to
// finish (each bounded by JobTimeout) and returns.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.promoteLoop(ctx)
	}()
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.consumeLoop(ctx)
		}()
	}
	wg.Wait()
	return nil
}

func (w *Worker) promoteLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.PromoteInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := w.broker.PromoteDue(ctx, time.Now()); err != nil && ctx.Err() == nil {
				w.cfg.Logger.Error("promote retries", "err", err)
			}
		}
	}
}

func (w *Worker) consumeLoop(ctx context.Context) {
	for ctx.Err() == nil {
		d, err := w.broker.Dequeue(ctx, w.cfg.Consumer, w.cfg.PollWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.cfg.Logger.Error("dequeue", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		if d != nil {
			w.process(ctx, d)
		}
	}
}

func (w *Worker) process(ctx context.Context, d *Delivery) {
	job := d.Job
	log := w.cfg.Logger.With("job_id", job.ID, "type", job.Type, "attempt", job.Attempt+1)

	var err error
	if d.Reclaimed {
		err = ErrLeaseExpired
	} else {
		err = w.run(ctx, job)
	}

	// Settle even if we are shutting down, so finished work is not redone.
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err == nil {
		if err := w.broker.Ack(opCtx, d); err != nil {
			log.Error("ack", "err", err)
		}
		return
	}

	job.Attempt++
	job.LastError = err.Error()
	maxAttempts := job.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if IsPermanent(err) || job.Attempt >= maxAttempts {
		if err := w.broker.DeadLetter(opCtx, d); err != nil {
			log.Error("dead-letter", "err", err)
			return
		}
		log.Warn("job dead-lettered", "err", job.LastError, "permanent", IsPermanent(err))
		return
	}
	delay := w.cfg.Backoff.Delay(job.Attempt)
	if err := w.broker.Retry(opCtx, d, time.Now().Add(delay)); err != nil {
		log.Error("schedule retry", "err", err)
		return
	}
	log.Info("job failed, retry scheduled", "err", job.LastError, "in", delay)
}

// run calls the handler with a timeout, converting panics into errors.
func (w *Worker) run(ctx context.Context, job *Job) (err error) {
	h, ok := w.handlers[job.Type]
	if !ok {
		return Permanent(fmt.Errorf("no handler for job type %q", job.Type))
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.JobTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(ctx, job)
}
