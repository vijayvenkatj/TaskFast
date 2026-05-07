package model

import "time"

// Task - User provided job
type Task struct {
	ID      uint32
	Payload []byte
	RunAt   time.Time
}

// TaskMeta - Server-side metadata for retries and limits
type TaskMeta struct {
	Task       *Task
	MaxRetries uint32
	Retries    uint32
}

type FetchOptions struct {
	WorkerID uint32
	TaskTime time.Duration
}
