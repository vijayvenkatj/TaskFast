package model

import "time"

// Task - User provided job
type Task struct {
	ID      uint32
	Payload []byte
	RunAt   time.Time
}

type TaskStatus string

const (
	Ready      TaskStatus = "READY"
	Delayed    TaskStatus = "DELAYED"
	Processing TaskStatus = "PROCESSING"
	DLQ        TaskStatus = "DLQ"
)

// TaskMeta - Server-side metadata for retries and limits
type TaskMeta struct {
	Task *Task

	Status TaskStatus

	Retries    int
	MaxRetries int

	Lease *Lease
}

type FetchOptions struct {
	WorkerID uint32
	TaskTime time.Duration
}

type Lease struct {
	WorkerID   uint32
	TaskID     uint32
	LeaseUntil time.Time
}
