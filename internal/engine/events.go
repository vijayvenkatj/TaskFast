package engine

import (
	"container/heap"
	"errors"
	"log"
	"time"

	"github.com/vijayvenkatj/taskfast/internal/model"
)

func (engine *EngineImpl) Apply(event *model.Event) (*Task, error) {

	engine.mu.Lock()
	defer engine.mu.Unlock()

	var taskMeta *TaskMeta
	var task *Task

	if event.Task != nil {
		taskMeta = event.Task
		if taskMeta != nil {
			task = taskMeta.Task
		}
	}

	var opts *FetchOptions
	if event.Opts != nil {
		opts = event.Opts
	}

	switch event.Type {

	case model.EnqueueEvent:

		// Backpressure
		if len(engine.ready)+len(engine.processing) > int(engine.limit) {
			return nil, errors.New("system overloaded!")
		}

		// Push the task into scheduled queue
		engine.tasks[task.ID] = &TaskMeta{
			Task:       task,
			Retries:    0,
			MaxRetries: 1,
		}
		heap.Push(&engine.scheduled, task)
		log.Println("Task", task.ID, "enqueued!")

	case model.FetchEvent:

		if len(engine.ready) == 0 {
			return nil, nil
		}

		task := engine.ready[0]
		engine.ready = engine.ready[1:]

		lease := NewLease(opts.WorkerID, task.ID, opts.TaskTime)
		engine.processing[task.ID] = lease
		log.Println("Task", task.ID, "fetched!")

		return task, nil

	case model.AckEvent:
		engine.completed = append(engine.completed, task)
		delete(engine.processing, task.ID)

		log.Println("Task", task.ID, "done!")

	case model.FailEvent:

		taskID := task.ID

		// Remove it from processing.
		delete(engine.processing, taskID)
		log.Println("Task", taskID, "Failed: ", event.Err)

		stored := engine.tasks[taskID]

		// Add the Task back to delayed or DLQ based on retries.
		if stored.Retries >= stored.MaxRetries {
			log.Println("Task", taskID, "moved to DLQ")
			engine.dlq = append(engine.dlq, task)
			return nil, nil
		}

		backoff := Backoff(50*time.Millisecond, stored.Retries)
		task.RunAt = time.Now().Add(backoff)
		stored.Retries += 1

		log.Println("Task", taskID, "retried!")
		heap.Push(&engine.scheduled, task)

	default:
		log.Println("Invalid EVENT")
		return nil, nil
	}

	return nil, nil
}
