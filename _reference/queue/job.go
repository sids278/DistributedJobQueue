// Package queue implements an at-least-once distributed job queue with
// retries (exponential backoff) and a dead-letter topic.
package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultMaxAttempts applies when a job does not set MaxAttempts.
const DefaultMaxAttempts = 5

var (
	// ErrNotFound is returned when a dead-lettered job does not exist.
	ErrNotFound = errors.New("queue: not found")
	// ErrLeaseExpired is recorded when a job is reclaimed from a worker that
	// held it past its lease (crashed, hung, or partitioned away).
	ErrLeaseExpired = errors.New("queue: lease expired before job was acknowledged")
)

// Job is the unit of work. It is serialized as JSON onto the broker.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Attempt     int             `json:"attempt"` // failed attempts so far
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	LastError   string          `json:"last_error,omitempty"`
}

// NewJob builds a job with a random ID. payload is JSON-encoded unless it is
// already a json.RawMessage or []byte.
func NewJob(typ string, payload any, maxAttempts int) (*Job, error) {
	var raw json.RawMessage
	switch p := payload.(type) {
	case nil:
	case json.RawMessage:
		raw = p
	case []byte:
		raw = p
	default:
		b, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("queue: encode payload: %w", err)
		}
		raw = b
	}
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	return &Job{
		ID:          newID(),
		Type:        typ,
		Payload:     raw,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

func (j *Job) clone() *Job {
	c := *j
	return &c
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Delivery is a job handed to a consumer. It must be settled exactly once via
// Ack, Retry or DeadLetter; otherwise it is reclaimed after the lease expires.
type Delivery struct {
	Job *Job
	// ID is the broker's handle for this delivery.
	ID string
	// Reclaimed is true when the job was taken over from a consumer whose
	// lease expired. The worker counts that as a failed attempt.
	Reclaimed bool
}

// DeadJob is an entry on the dead-letter topic.
type DeadJob struct {
	ID  string
	Job *Job
}

// Broker is the storage/transport behind the queue. Implementations must be
// safe for concurrent use by many workers, possibly in different processes.
type Broker interface {
	Enqueue(ctx context.Context, job *Job) error
	// Dequeue waits up to wait for a job. It returns (nil, nil) on timeout.
	Dequeue(ctx context.Context, consumer string, wait time.Duration) (*Delivery, error)
	Ack(ctx context.Context, d *Delivery) error
	// Retry removes d from in-flight and schedules d.Job to be redelivered at at.
	Retry(ctx context.Context, d *Delivery, at time.Time) error
	// DeadLetter removes d from in-flight and publishes d.Job to the dead-letter topic.
	DeadLetter(ctx context.Context, d *Delivery) error
	// PromoteDue moves scheduled retries whose time has come back onto the
	// main topic and reports how many were moved.
	PromoteDue(ctx context.Context, now time.Time) (int, error)
	ListDead(ctx context.Context, limit int) ([]DeadJob, error)
	// Replay moves a dead-lettered job back onto the main topic with its
	// attempt counter reset.
	Replay(ctx context.Context, deadID string) error
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable: the job goes straight to the
// dead-letter topic.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
