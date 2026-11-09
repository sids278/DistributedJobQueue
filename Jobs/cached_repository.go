package Jobs

import (
	"errors"
	"sync"
)

// CachedJobRepository wraps a repository with a read-through cache. Writes
// through the wrapper keep cached job copies synchronized with the storage.
type CachedJobRepository struct {
	mu         sync.RWMutex
	repository JobResps
	jobs       map[int64]*Job
}

var _ JobResps = (*CachedJobRepository)(nil)

// NewCachedJobRepository wraps repository with an empty in-memory cache.
func NewCachedJobRepository(repository JobResps) (*CachedJobRepository, error) {
	if repository == nil {
		return nil, errors.New("cannot cache a nil job repository")
	}
	return &CachedJobRepository{
		repository: repository,
		jobs:       make(map[int64]*Job),
	}, nil
}

func (r *CachedJobRepository) GetJobById(id int64) (*Job, error) {
	r.mu.RLock()
	if job, ok := r.jobs[id]; ok {
		defer r.mu.RUnlock()
		return cloneJob(job), nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if job, ok := r.jobs[id]; ok {
		return cloneJob(job), nil
	}

	job, err := r.repository.GetJobById(id)
	if err != nil {
		return nil, err
	}
	r.jobs[id] = cloneJob(job)
	return cloneJob(job), nil
}

func (r *CachedJobRepository) GetAllJobs() ([]*Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	jobs, err := r.repository.GetAllJobs()
	if err != nil {
		return nil, err
	}
	cachedJobs := make(map[int64]*Job, len(jobs))
	for _, job := range jobs {
		cachedJobs[job.Id] = cloneJob(job)
	}
	r.jobs = cachedJobs
	return cloneJobs(jobs), nil
}

func (r *CachedJobRepository) CreateJob(job *Job) (int64, error) {
	if job == nil {
		return 0, errors.New("cannot create a nil job")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	id, err := r.repository.CreateJob(job)
	if err != nil {
		return 0, err
	}
	cached := cloneJob(job)
	cached.Id = id
	r.jobs[id] = cached
	return id, nil
}

func (r *CachedJobRepository) UpdateJob(job *Job) error {
	if job == nil {
		return errors.New("cannot update a nil job")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.repository.UpdateJob(job); err != nil {
		return err
	}
	r.jobs[job.Id] = cloneJob(job)
	return nil
}

func (r *CachedJobRepository) DeleteJob(id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.repository.DeleteJob(id); err != nil {
		return err
	}
	delete(r.jobs, id)
	return nil
}

func (r *CachedJobRepository) RunJob(id int64) error {
	job, err := r.GetJobById(id)
	if err != nil {
		return err
	}
	return RunSqlJob(job.Query)
}

func (r *CachedJobRepository) isSchedulable() bool {
	return r.repository.isSchedulable()
}

func cloneJobs(jobs []*Job) []*Job {
	clones := make([]*Job, 0, len(jobs))
	for _, job := range jobs {
		clones = append(clones, cloneJob(job))
	}
	return clones
}
