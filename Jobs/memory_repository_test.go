package Jobs

import (
	"errors"
	"testing"
)

func TestMemoryJobRepositoryCRUD(t *testing.T) {
	repository := NewMemoryJobRepository()

	firstID, err := repository.CreateJob(&Job{Desc: "first", Query: "SELECT 1"})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	secondID, err := repository.CreateJob(&Job{Id: 10, Desc: "second", Query: "SELECT 2"})
	if err != nil {
		t.Fatalf("CreateJob() with explicit ID error = %v", err)
	}

	jobs, err := repository.GetAllJobs()
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	if len(jobs) != 2 || jobs[0].Id != firstID || jobs[1].Id != secondID {
		t.Fatalf("GetAllJobs() = %#v, want jobs ordered by ID", jobs)
	}

	jobs[0].Query = "modified outside repository"
	stored, err := repository.GetJobById(firstID)
	if err != nil {
		t.Fatalf("GetJobById() error = %v", err)
	}
	if stored.Query != "SELECT 1" {
		t.Fatalf("GetJobById() query = %q, want original query", stored.Query)
	}

	stored.Query = "SELECT 3"
	if err := repository.UpdateJob(stored); err != nil {
		t.Fatalf("UpdateJob() error = %v", err)
	}
	stored, err = repository.GetJobById(firstID)
	if err != nil {
		t.Fatalf("GetJobById() after update error = %v", err)
	}
	if stored.Query != "SELECT 3" {
		t.Fatalf("updated query = %q, want SELECT 3", stored.Query)
	}

	if err := repository.DeleteJob(firstID); err != nil {
		t.Fatalf("DeleteJob() error = %v", err)
	}
	if _, err := repository.GetJobById(firstID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("GetJobById() after delete error = %v, want ErrJobNotFound", err)
	}
}

func TestMemoryJobRepositoryRunJobReportsUnavailableExecutor(t *testing.T) {
	repository := NewMemoryJobRepository()
	id, err := repository.CreateJob(&Job{Query: "SELECT 1"})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	if err := repository.RunJob(id); !errors.Is(err, ErrSQLNotImplemented) {
		t.Fatalf("RunJob() error = %v, want ErrSQLNotImplemented", err)
	}
}
