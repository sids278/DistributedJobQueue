package main

import (
	"fmt"
	"jobqueue/Jobs"
)

func main() {
	storage := Jobs.NewMemoryJobRepository()
	jobRepositoryCache, err := Jobs.NewCachedJobRepository(storage)
	if err != nil {
		fmt.Println("Could not initialize job cache:", err)
		return
	}
	var jobRepository Jobs.JobResps = jobRepositoryCache

	sqlJob := &Jobs.Job{
		Desc:  "Example scheduled SQL job",
		Query: "SELECT 1",
	}
	id, err := jobRepository.CreateJob(sqlJob)
	if err != nil {
		fmt.Println("Could not create SQL job:", err)
		return
	}

	storedJob, err := jobRepository.GetJobById(id)
	if err != nil {
		fmt.Println("Could not retrieve SQL job:", err)
		return
	}
	fmt.Printf("Stored SQL job %d: %s\n", storedJob.Id, storedJob.Query)
}
