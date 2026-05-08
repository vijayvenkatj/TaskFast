package engine

import (
	"time"
)

// Min heap for the next task to be fetched
type ScheduleHeap []*Task

func (h ScheduleHeap) Len() int { return len(h) }
func (h ScheduleHeap) Less(i, j int) bool {
	return h[i].RunAt.Before(h[j].RunAt)
}
func (h ScheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ScheduleHeap) Push(x any) {
	*h = append(*h, x.(*Task))
}
func (h *ScheduleHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Create Lease for the processing task
func NewLease(workerID, taskID uint32, leaseUntil time.Time) *Lease {
	return &Lease{
		WorkerID:   workerID,
		TaskID:     taskID,
		LeaseUntil: leaseUntil,
	}
}

func Backoff(base time.Duration, retries int) time.Duration {
	return base * time.Duration(1<<retries)
}
