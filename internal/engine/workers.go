package engine

import (
	"container/heap"
	"log"
	"time"

	"github.com/vijayvenkatj/taskfast/internal/model"
)

// Reaper -> checks for expired leases.
func (engine *EngineImpl) Reaper() {
	for {
		engine.mu.Lock()
		now := time.Now()
		for taskID, lease := range engine.processing {
			taskMeta := engine.tasks[taskID]
			if taskMeta == nil {
				delete(engine.processing, taskID)
				continue
			}

			if now.After(lease.LeaseUntil) {
				moveToDLQ := taskMeta.Retries >= taskMeta.MaxRetries
				retryCount := taskMeta.Retries
				runAt := taskMeta.Task.RunAt
				if !moveToDLQ {
					backoff := Backoff(50*time.Millisecond, taskMeta.Retries)
					runAt = now.Add(backoff)
					retryCount = taskMeta.Retries + 1
				}

				event := model.Event{
					Type: model.FailEvent,
					Fail: &model.FailEventData{
						TaskID:     taskID,
						RetryCount: retryCount,
						RunAt:      runAt,
						MoveToDLQ:  moveToDLQ,
						Error:      "lease expired",
					},
				}

				if err := engine.wal.Append(event); err != nil {
					log.Println("ERROR appending reaper event:", err)
					continue
				}

				if err := engine.Apply(&event); err != nil {
					log.Println("ERROR applying reaper event:", err)
					continue
				}

				delete(engine.processing, taskID)

				if moveToDLQ {
					engine.dlq = append(engine.dlq, taskMeta.Task)
					continue
				}

				if runAt.After(now) {
					heap.Push(&engine.scheduled, taskMeta.Task)
				} else {
					engine.ready = append(engine.ready, taskMeta.Task)
				}
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
