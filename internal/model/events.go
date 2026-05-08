package model

import "time"

type EventType string

const (
	EnqueueEvent EventType = "ENQUEUE"
	FetchEvent   EventType = "FETCH"
	AckEvent     EventType = "ACK"
	FailEvent    EventType = "FAIL"
)

type EnqueueEventData struct {
	Task       Task       `json:"task"`
	Status     TaskStatus `json:"status"`
	Retries    int        `json:"retries"`
	MaxRetries int        `json:"max_retries"`
}

type FetchEventData struct {
	TaskID      uint32    `json:"task_id"`
	WorkerID    uint32    `json:"worker_id"`
	LeaseExpiry time.Time `json:"lease_expiry"`
}

type FailEventData struct {
	TaskID     uint32    `json:"task_id"`
	RetryCount int       `json:"retry_count"`
	RunAt      time.Time `json:"run_at"`
	MoveToDLQ  bool      `json:"move_to_dlq"`
	Error      string    `json:"error"`
}

type AckEventData struct {
	TaskID uint32 `json:"task_id"`
}

type Event struct {
	Type EventType `json:"type"`

	Enqueue *EnqueueEventData `json:"enqueue,omitempty"`
	Fetch   *FetchEventData   `json:"fetch,omitempty"`
	Fail    *FailEventData    `json:"fail,omitempty"`
	Ack     *AckEventData     `json:"ack,omitempty"`
}
