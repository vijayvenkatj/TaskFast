package engine

import (
	"errors"

	"github.com/vijayvenkatj/taskfast/internal/model"
)

func (engine *EngineImpl) Apply(event *model.Event) error {
	switch event.Type {
	case model.EnqueueEvent:
		if event.Enqueue == nil {
			return errors.New("missing enqueue event data")
		}

		task := event.Enqueue.Task
		engine.tasks[task.ID] = &TaskMeta{
			Task:       &task,
			Status:     event.Enqueue.Status,
			Retries:    event.Enqueue.Retries,
			MaxRetries: event.Enqueue.MaxRetries,
			Lease:      nil,
		}

	case model.FetchEvent:
		if event.Fetch == nil {
			return errors.New("missing fetch event data")
		}

		stored := engine.tasks[event.Fetch.TaskID]
		if stored == nil {
			return errors.New("task not found for fetch event")
		}

		stored.Status = model.Processing
		stored.Lease = &Lease{
			WorkerID:   event.Fetch.WorkerID,
			TaskID:     event.Fetch.TaskID,
			LeaseUntil: event.Fetch.LeaseExpiry,
		}

	case model.AckEvent:
		if event.Ack == nil {
			return errors.New("missing ack event data")
		}

		delete(engine.tasks, event.Ack.TaskID)

	case model.FailEvent:
		if event.Fail == nil {
			return errors.New("missing fail event data")
		}

		stored := engine.tasks[event.Fail.TaskID]
		if stored == nil {
			return errors.New("task not found for fail event")
		}

		stored.Retries = event.Fail.RetryCount
		stored.Task.RunAt = event.Fail.RunAt
		stored.Lease = nil
		if event.Fail.MoveToDLQ {
			stored.Status = model.DLQ
		} else {
			stored.Status = model.Delayed
		}

	default:
		return errors.New("invalid event type")
	}

	return nil
}
