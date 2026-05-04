package engine

import (
	"container/heap"
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
func NewLease(workerID, taskID uint32, duration time.Duration) *Lease {
	return &Lease{
		WorkerID:   workerID,
		TaskID:     taskID,
		LeaseUntil: time.Now().Add(duration),
	}
}

func Backoff(base time.Duration, retries uint32) time.Duration {
	return base * time.Duration(1<<retries)
}

// Reaper -> checks for expired leases.
func (engine *EngineImpl) Reaper() {
	for {
		for taskID, lease := range engine.processing {
			task := engine.tasks[taskID]

			if time.Now().After(lease.LeaseUntil) {
				// Remove it from processing.
				delete(engine.processing, taskID)

				// Add the Task back to delayed or DLQ based on retries.
				if task.Retries >= task.MaxRetries {
					engine.dlq = append(engine.dlq, task)
					continue
				}

				backoff := Backoff(50*time.Millisecond, task.Retries)
				task.RunAt = time.Now().Add(backoff)

				heap.Push(&engine.scheduled, task)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
