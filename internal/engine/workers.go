package engine

import (
	"container/heap"
	"time"
)

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

// Scheduler -> checks for ready tasks
func (engine *EngineImpl) Scheduler() {
	for {
		engine.mu.RLock()
		if len(engine.scheduled) == 0 {
			engine.mu.RUnlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}

		next := engine.scheduled[0].RunAt
		now := time.Now()

		if next.After(now) {
			sleep := next.Sub(now)
			engine.mu.RUnlock()
			time.Sleep(sleep)
			continue
		}

		engine.mu.Lock()
		for engine.scheduled.Len() > 0 && !engine.scheduled[0].RunAt.After(now) {
			task := heap.Pop(&engine.scheduled).(*Task)
			engine.ready = append(engine.ready, task)
		}
		engine.mu.Unlock()
	}
}
