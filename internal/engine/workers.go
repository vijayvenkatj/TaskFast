package engine

import (
	"container/heap"
	"time"
)

// Reaper -> checks for expired leases.
func (engine *EngineImpl) Reaper() {
	for {
		engine.mu.Lock()
		for taskID, lease := range engine.processing {
			taskMeta := engine.tasks[taskID]
			if taskMeta == nil {
				delete(engine.processing, taskID)
				continue
			}

			if time.Now().After(lease.LeaseUntil) {
				// Remove it from processing.
				delete(engine.processing, taskID)

				// Add the Task back to delayed or DLQ based on retries.
				if taskMeta.Retries >= taskMeta.MaxRetries {
					engine.dlq = append(engine.dlq, taskMeta.Task)
					continue
				}

				backoff := Backoff(50*time.Millisecond, taskMeta.Retries)
				taskMeta.Task.RunAt = time.Now().Add(backoff)
				taskMeta.Retries += 1

				heap.Push(&engine.scheduled, taskMeta.Task)
			}
		}
		engine.mu.Unlock()
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

		engine.mu.RUnlock()

		engine.mu.Lock()
		for engine.scheduled.Len() > 0 && !engine.scheduled[0].RunAt.After(now) {
			task := heap.Pop(&engine.scheduled).(*Task)
			engine.ready = append(engine.ready, task)
		}
		engine.mu.Unlock()
	}
}
