package Jobs

import (
	"errors"
	"testing"
)

type countingJobRepository struct {
	JobResps
	getCalls int
}

func (r *countingJobRepository) GetJobById(id int64) (*Job, error) {
	r.getCalls++
	return r.JobResps.GetJobById(id)
}

func TestCachedJobRepositoryReadThroughAndCopies(t *testing.T) {
	store := NewMemoryJobRepository()
	id, err := store.CreateJob(&Job{Query: "SELECT 1"})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	countingStore := &countingJobRepository{JobResps: store}
	repository, err := NewCachedJobRepository(countingStore)
	if err != nil {
		t.Fatalf("NewCachedJobRepository() error = %v", err)
	}

	first, err := repository.GetJobById(id)
	if err != nil {
		t.Fatalf("first GetJobById() error = %v", err)
	}
	first.Query = "changed by caller"
	second, err := repository.GetJobById(id)
	if err != nil {
		t.Fatalf("second GetJobById() error = %v", err)
	}

	if countingStore.getCalls != 1 {
		t.Fatalf("backing GetJobById() calls = %d, want 1", countingStore.getCalls)
	}
	if second.Query != "SELECT 1" {
		t.Fatalf("cached query = %q, want SELECT 1", second.Query)
	}
}

func TestCachedJobRepositoryRefreshesAndInvalidatesOnWrites(t *testing.T) {
	store := NewMemoryJobRepository()
	id, err := store.CreateJob(&Job{Query: "SELECT 1"})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	repository, err := NewCachedJobRepository(store)
	if err != nil {
		t.Fatalf("NewCachedJobRepository() error = %v", err)
	}
	if _, err := repository.GetJobById(id); err != nil {
		t.Fatalf("initial GetJobById() error = %v", err)
	}

	if err := repository.UpdateJob(&Job{Id: id, Query: "SELECT 2"}); err != nil {
		t.Fatalf("UpdateJob() error = %v", err)
	}
	updated, err := repository.GetJobById(id)
	if err != nil {
		t.Fatalf("GetJobById() after update error = %v", err)
	}
	if updated.Query != "SELECT 2" {
		t.Fatalf("query after update = %q, want SELECT 2", updated.Query)
	}

	if err := repository.DeleteJob(id); err != nil {
		t.Fatalf("DeleteJob() error = %v", err)
	}
	if _, err := repository.GetJobById(id); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("GetJobById() after delete error = %v, want ErrJobNotFound", err)
	}
}

func TestNewCachedJobRepositoryRejectsNilRepository(t *testing.T) {
	if _, err := NewCachedJobRepository(nil); err == nil {
		t.Fatal("NewCachedJobRepository(nil) error = nil, want an error")
	}
}
