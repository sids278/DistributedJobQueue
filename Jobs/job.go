package Jobs

type Job struct {
	Id    int64
	Desc  string
	Query string
}

type JobResps interface {
	GetJobById(id int64) (*Job, error)
	GetAllJobs() ([]*Job, error)
	CreateJob(job *Job) (int64, error)
	UpdateJob(job *Job) error
	DeleteJob(id int64) error
	RunJob(id int64) error
	isSchedulable() bool
}
