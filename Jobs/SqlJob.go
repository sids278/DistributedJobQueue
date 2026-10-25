package Jobs

type ScheduledSqlJob struct {
	JobId        int64
	Query        string
	Schedule     string
	TimeInterval int64
}

func (s ScheduledSqlJob) Run() error {
	return RunSqlJob(s.Query)
}
