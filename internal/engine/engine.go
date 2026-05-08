package engine

import (
	"container/heap"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/vijayvenkatj/taskfast/internal/model"
	"github.com/vijayvenkatj/taskfast/internal/storage"
)

type Engine interface {
	Enqueue(task *Task) error
	Fetch(opts FetchOptions) *Task

	Ack(taskID uint32) error
	Fail(taskID uint32, err error) error

	DLQ() []Task
}

type EngineImpl struct {
	mu sync.RWMutex

	tasks     map[uint32]*TaskMeta
	completed []*Task
	limit     uint32

	// Queues
	ready      []*Task
	processing map[uint32]*Lease
	scheduled  ScheduleHeap
	dlq        []*Task

	wal *storage.WAL
}

func (engine *EngineImpl) Enqueue(task *Task) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	// Backpressure
	if len(engine.ready)+len(engine.processing) > int(engine.limit) {
		return errors.New("system overloaded!")
	}

	now := time.Now()
	status := model.Ready
	if task.RunAt.After(now) {
		status = model.Delayed
	}

	event := model.Event{
		Type: model.EnqueueEvent,
		Enqueue: &model.EnqueueEventData{
			Task:       *task,
			Status:     status,
			Retries:    0,
			MaxRetries: 1,
		},
	}

	if err := engine.wal.Append(event); err != nil {
		return err
	}

	if err := engine.Apply(&event); err != nil {
		return err
	}

	stored := engine.tasks[task.ID]
	if stored == nil {
		return errors.New("task not found after enqueue")
	}

	if status == model.Ready {
		engine.ready = append(engine.ready, stored.Task)
	} else {
		heap.Push(&engine.scheduled, stored.Task)
	}

	log.Println("Task", task.ID, "enqueued!")
	return nil
}

func (engine *EngineImpl) Fetch(opts FetchOptions) *Task {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	for len(engine.ready) > 0 {
		task := engine.ready[0]
		engine.ready = engine.ready[1:]

		stored := engine.tasks[task.ID]
		if stored == nil {
			continue
		}

		leaseExpiry := time.Now().Add(opts.TaskTime)
		event := model.Event{
			Type: model.FetchEvent,
			Fetch: &model.FetchEventData{
				TaskID:      task.ID,
				WorkerID:    opts.WorkerID,
				LeaseExpiry: leaseExpiry,
			},
		}

		if err := engine.wal.Append(event); err != nil {
			return nil
		}

		if err := engine.Apply(&event); err != nil {
			return nil
		}

		engine.processing[task.ID] = engine.tasks[task.ID].Lease

		return engine.tasks[task.ID].Task
	}

	return nil
}

func (engine *EngineImpl) Ack(taskID uint32) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	stored := engine.tasks[taskID]
	if stored == nil {
		return errors.New("task not found")
	}

	event := model.Event{
		Type: model.AckEvent,
		Ack: &model.AckEventData{
			TaskID: taskID,
		},
	}

	if err := engine.wal.Append(event); err != nil {
		return err
	}

	if err := engine.Apply(&event); err != nil {
		return err
	}

	engine.completed = append(engine.completed, stored.Task)
	delete(engine.processing, taskID)

	log.Println("Task", taskID, "done!")
	return nil
}

func (engine *EngineImpl) Fail(taskID uint32, failure error) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	stored := engine.tasks[taskID]
	if stored == nil {
		return errors.New("task not found")
	}

	// Remove it from processing.
	delete(engine.processing, taskID)
	log.Println("Task", taskID, "Failed")

	moveToDLQ := stored.Retries >= stored.MaxRetries
	retryCount := stored.Retries
	runAt := stored.Task.RunAt
	if !moveToDLQ {
		backoff := Backoff(50*time.Millisecond, stored.Retries)
		runAt = time.Now().Add(backoff)
		retryCount = stored.Retries + 1
	}

	errMsg := ""
	if failure != nil {
		errMsg = failure.Error()
	}

	event := model.Event{
		Type: model.FailEvent,
		Fail: &model.FailEventData{
			TaskID:     taskID,
			RetryCount: retryCount,
			RunAt:      runAt,
			MoveToDLQ:  moveToDLQ,
			Error:      errMsg,
		},
	}

	if err := engine.wal.Append(event); err != nil {
		return err
	}

	if err := engine.Apply(&event); err != nil {
		return err
	}

	if moveToDLQ {
		log.Println("Task", taskID, "moved to DLQ")
		engine.dlq = append(engine.dlq, stored.Task)
		return nil
	}

	if runAt.After(time.Now()) {
		heap.Push(&engine.scheduled, stored.Task)
	} else {
		engine.ready = append(engine.ready, stored.Task)
	}

	log.Println("Task", taskID, "retried!")
	return nil
}

func (engine *EngineImpl) DLQ() []Task {
	var deadTasks []Task
	for _, task := range engine.dlq {
		deadTasks = append(deadTasks, *task)
	}
	return deadTasks
}

// Constructor for our Engine
func NewEngine(logPath string) Engine {
	wal, err := storage.NewWAL(logPath)
	if err != nil {
		log.Println("ERROR creating WAL")
		return nil
	}

	engine := &EngineImpl{
		tasks: make(map[uint32]*TaskMeta),
		limit: 100,

		ready:     []*Task{},
		completed: []*Task{},
		dlq:       []*Task{},

		processing: make(map[uint32]*Lease),
		scheduled:  ScheduleHeap{},

		wal: wal,
	}

	engine.Restore()

	go engine.Reaper()
	go engine.Scheduler()

	return engine
}

func (engine *EngineImpl) Restore() {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	events, err := engine.wal.Replay()
	if err != nil {
		log.Println("ERROR replaying WAL:", err)
		return
	}

	engine.tasks = make(map[uint32]*TaskMeta)
	engine.ready = []*Task{}
	engine.processing = make(map[uint32]*Lease)
	engine.scheduled = ScheduleHeap{}
	engine.dlq = []*Task{}
	engine.completed = []*Task{}
	containerInit := &engine.scheduled
	heap.Init(containerInit)

	for _, event := range events {
		if err := engine.Apply(&event); err != nil {
			log.Println("ERROR applying event:", err)
		}
	}

	now := time.Now()
	for _, meta := range engine.tasks {
		if meta.Lease != nil && !meta.Lease.LeaseUntil.After(now) {
			meta.Lease = nil
		}

		switch meta.Status {
		case model.Processing:
			meta.Status = model.Ready
			meta.Lease = nil
		case model.Delayed:
			if !meta.Task.RunAt.After(now) {
				meta.Status = model.Ready
			}
		}
	}

	for _, meta := range engine.tasks {
		switch meta.Status {
		case model.Ready:
			engine.ready = append(engine.ready, meta.Task)
		case model.Delayed:
			heap.Push(&engine.scheduled, meta.Task)
		case model.DLQ:
			engine.dlq = append(engine.dlq, meta.Task)
		}
	}

	log.Println("Restore complete. Tasks:", len(engine.tasks))
}
