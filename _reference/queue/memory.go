package queue

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// MemoryBroker is an in-process Broker with the same semantics as
// RedisBroker. It is meant for tests and local development.
type MemoryBroker struct {
	lease  time.Duration
	notify chan struct{}

	mu       sync.Mutex
	seq      int
	ready    []*Job
	inflight map[string]*leasedJob
	retries  []scheduledJob
	dead     []DeadJob
}

type leasedJob struct {
	job      *Job
	deadline time.Time
}

type scheduledJob struct {
	job *Job
	at  time.Time
}

// NewMemoryBroker returns a broker whose deliveries are reclaimable once they
// have been in flight longer than lease.
func NewMemoryBroker(lease time.Duration) *MemoryBroker {
	return &MemoryBroker{
		lease:    lease,
		notify:   make(chan struct{}, 1),
		inflight: map[string]*leasedJob{},
	}
}

func (m *MemoryBroker) nextID() string {
	m.seq++
	return strconv.Itoa(m.seq)
}

func (m *MemoryBroker) wake() {
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

func (m *MemoryBroker) Enqueue(_ context.Context, job *Job) error {
	m.mu.Lock()
	m.ready = append(m.ready, job.clone())
	m.mu.Unlock()
	m.wake()
	return nil
}

func (m *MemoryBroker) Dequeue(ctx context.Context, _ string, wait time.Duration) (*Delivery, error) {
	deadline := time.Now().Add(wait)
	for {
		if d := m.tryDequeue(); d != nil {
			return d, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		// Poll periodically as well so expired leases are noticed.
		if remaining > 20*time.Millisecond {
			remaining = 20 * time.Millisecond
		}
		t := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-m.notify:
			t.Stop()
		case <-t.C:
		}
	}
}

func (m *MemoryBroker) tryDequeue() *Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for id, l := range m.inflight {
		if now.After(l.deadline) {
			l.deadline = now.Add(m.lease)
			return &Delivery{Job: l.job.clone(), ID: id, Reclaimed: true}
		}
	}
	if len(m.ready) == 0 {
		return nil
	}
	job := m.ready[0]
	m.ready = m.ready[1:]
	id := m.nextID()
	m.inflight[id] = &leasedJob{job: job, deadline: now.Add(m.lease)}
	return &Delivery{Job: job.clone(), ID: id}
}

func (m *MemoryBroker) Ack(_ context.Context, d *Delivery) error {
	m.mu.Lock()
	delete(m.inflight, d.ID)
	m.mu.Unlock()
	return nil
}

func (m *MemoryBroker) Retry(_ context.Context, d *Delivery, at time.Time) error {
	m.mu.Lock()
	delete(m.inflight, d.ID)
	m.retries = append(m.retries, scheduledJob{job: d.Job.clone(), at: at})
	m.mu.Unlock()
	return nil
}

func (m *MemoryBroker) DeadLetter(_ context.Context, d *Delivery) error {
	m.mu.Lock()
	delete(m.inflight, d.ID)
	m.dead = append(m.dead, DeadJob{ID: "dead-" + m.nextID(), Job: d.Job.clone()})
	m.mu.Unlock()
	return nil
}

func (m *MemoryBroker) PromoteDue(_ context.Context, now time.Time) (int, error) {
	m.mu.Lock()
	kept := m.retries[:0]
	moved := 0
	for _, s := range m.retries {
		if s.at.After(now) {
			kept = append(kept, s)
			continue
		}
		m.ready = append(m.ready, s.job)
		moved++
	}
	m.retries = kept
	m.mu.Unlock()
	if moved > 0 {
		m.wake()
	}
	return moved, nil
}

func (m *MemoryBroker) ListDead(_ context.Context, limit int) ([]DeadJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.dead)
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]DeadJob, n)
	for i := range out {
		out[i] = DeadJob{ID: m.dead[i].ID, Job: m.dead[i].Job.clone()}
	}
	return out, nil
}

func (m *MemoryBroker) Replay(_ context.Context, deadID string) error {
	m.mu.Lock()
	for i, dj := range m.dead {
		if dj.ID != deadID {
			continue
		}
		m.dead = append(m.dead[:i], m.dead[i+1:]...)
		m.ready = append(m.ready, resetForReplay(dj.Job))
		m.mu.Unlock()
		m.wake()
		return nil
	}
	m.mu.Unlock()
	return ErrNotFound
}

func resetForReplay(j *Job) *Job {
	c := j.clone()
	c.Attempt = 0
	c.LastError = ""
	return c
}
