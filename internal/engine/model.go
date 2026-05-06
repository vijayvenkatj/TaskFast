package engine

import "time"

// Task - User provided JOB
type Task struct {
	ID      uint32
	Payload []byte
	RunAt   time.Time
}
type TaskMeta struct {
	Task       *Task
	MaxRetries uint32
	Retries    uint32
}

type Lease struct {
	WorkerID   uint32
	TaskID     uint32
	LeaseUntil time.Time
}

type FetchOptions struct {
	WorkerID uint32
	TaskTime time.Duration
}
