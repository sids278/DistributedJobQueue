package Jobs

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrJobNotFound       = errors.New("job not found")
	ErrJobAlreadyExists  = errors.New("job already exists")
	ErrJobQueryIsEmpty   = errors.New("job query is empty")
	ErrSQLNotImplemented = errors.New("SQL execution is not implemented: no database executor is configured")
)

type MemoryJobRepository struct {
	mu     sync.RWMutex
	jobs   []*Job
	nextID int64
}

var _ JobResps = (*MemoryJobRepository)(nil)

func NewMemoryJobRepository() *MemoryJobRepository {
	return &MemoryJobRepository{
		jobs:   make([]*Job, 0),
		nextID: 1,
	}
}

func (r *MemoryJobRepository) GetJobById(id int64) (*Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	index := r.findJobIndex(id)
	if index == -1 {
		return nil, ErrJobNotFound
	}
	return cloneJob(r.jobs[index]), nil
}

func (r *MemoryJobRepository) GetAllJobs() ([]*Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobs := make([]*Job, 0, len(r.jobs))
	for _, job := range r.jobs {
		jobs = append(jobs, cloneJob(job))
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].Id < jobs[j].Id })
	return jobs, nil
}

func (r *MemoryJobRepository) CreateJob(job *Job) (int64, error) {
	if job == nil {
		return 0, errors.New("cannot create a nil job")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	copy := cloneJob(job)
	if copy.Id == 0 {
		for r.findJobIndex(r.nextID) != -1 {
			r.nextID++
		}
		copy.Id = r.nextID
		r.nextID++
	} else if r.findJobIndex(copy.Id) != -1 {
		return 0, fmt.Errorf("%w: %d", ErrJobAlreadyExists, copy.Id)
	} else if copy.Id >= r.nextID {
		r.nextID = copy.Id + 1
	}

	r.jobs = append(r.jobs, copy)
	return copy.Id, nil
}

func (r *MemoryJobRepository) UpdateJob(job *Job) error {
	if job == nil {
		return errors.New("cannot update a nil job")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	index := r.findJobIndex(job.Id)
	if index == -1 {
		return ErrJobNotFound
	}
	r.jobs[index] = cloneJob(job)
	return nil
}

func (r *MemoryJobRepository) DeleteJob(id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	index := r.findJobIndex(id)
	if index == -1 {
		return ErrJobNotFound
	}
	copy(r.jobs[index:], r.jobs[index+1:])
	r.jobs[len(r.jobs)-1] = nil
	r.jobs = r.jobs[:len(r.jobs)-1]
	return nil
}

func (r *MemoryJobRepository) RunJob(id int64) error {
	job, err := r.GetJobById(id)
	if err != nil {
		return err
	}
	return RunSqlJob(job.Query)
}

func (r *MemoryJobRepository) isSchedulable() bool {
	return false
}

func cloneJob(job *Job) *Job {
	copy := *job
	return &copy
}

func (r *MemoryJobRepository) findJobIndex(id int64) int {
	for i, job := range r.jobs {
		if job.Id == id {
			return i
		}
	}
	return -1
}

func RunSqlJob(query string) error {
	if strings.TrimSpace(query) == "" {
		return ErrJobQueryIsEmpty
	}
	return ErrSQLNotImplemented
}
