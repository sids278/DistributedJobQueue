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
	jobs   map[int64]*Job
	nextID int64
}

var _ JobResps = (*MemoryJobRepository)(nil)

func NewMemoryJobRepository() *MemoryJobRepository {
	return &MemoryJobRepository{
		jobs:   make(map[int64]*Job),
		nextID: 1,
	}
}

func (r *MemoryJobRepository) GetJobById(id int64) (*Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	job, ok := r.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return cloneJob(job), nil
}

func (r *MemoryJobRepository) GetAllJobs() ([]*Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]int64, 0, len(r.jobs))
	for id := range r.jobs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	jobs := make([]*Job, 0, len(ids))
	for _, id := range ids {
		jobs = append(jobs, cloneJob(r.jobs[id]))
	}
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
		for r.jobs[r.nextID] != nil {
			r.nextID++
		}
		copy.Id = r.nextID
		r.nextID++
	} else if _, exists := r.jobs[copy.Id]; exists {
		return 0, fmt.Errorf("%w: %d", ErrJobAlreadyExists, copy.Id)
	} else if copy.Id >= r.nextID {
		r.nextID = copy.Id + 1
	}

	r.jobs[copy.Id] = copy
	return copy.Id, nil
}

func (r *MemoryJobRepository) UpdateJob(job *Job) error {
	if job == nil {
		return errors.New("cannot update a nil job")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.jobs[job.Id]; !exists {
		return ErrJobNotFound
	}
	r.jobs[job.Id] = cloneJob(job)
	return nil
}

func (r *MemoryJobRepository) DeleteJob(id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.jobs[id]; !exists {
		return ErrJobNotFound
	}
	delete(r.jobs, id)
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

func RunSqlJob(query string) error {
	if strings.TrimSpace(query) == "" {
		return ErrJobQueryIsEmpty
	}
	return ErrSQLNotImplemented
}
