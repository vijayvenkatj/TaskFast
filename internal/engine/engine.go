package engine

import (
	"container/heap"
	"log"
	"sync"
	"time"
)

type Engine interface {
	Enqueue(task *Task) error
	Fetch(opts FetchOptions) *Task

	Ack(task *Task) error
	Fail(task *Task, err error) error

	DLQ() []Task
}

type EngineImpl struct {
	mu sync.RWMutex

	tasks     map[uint32]*TaskMeta
	completed []*Task

	// Queues
	ready      []*Task
	processing map[uint32]*Lease
	scheduled  ScheduleHeap
	dlq        []*Task
}

func (engine *EngineImpl) Enqueue(task *Task) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	// TODO: Idempotency logic
	engine.tasks[task.ID] = &TaskMeta{
		Task:       task,
		Retries:    0,
		MaxRetries: 1,
	}
	heap.Push(&engine.scheduled, task)
	log.Println("Task", task.ID, "enqueued!")

	return nil
}
func (engine *EngineImpl) Fetch(opts FetchOptions) *Task {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if len(engine.ready) == 0 {
		return nil
	}

	task := engine.ready[0]
	engine.ready = engine.ready[1:]

	lease := NewLease(opts.WorkerID, task.ID, opts.TaskTime)
	engine.processing[task.ID] = lease
	log.Println("Task", task.ID, "fetched!")

	return task
}

func (engine *EngineImpl) Ack(task *Task) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	engine.completed = append(engine.completed, task)
	delete(engine.processing, task.ID)

	log.Println("Task", task.ID, "done!")

	return nil
}
func (engine *EngineImpl) Fail(task *Task, err error) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	taskID := task.ID

	// Remove it from processing.
	delete(engine.processing, taskID)
	log.Println("Task", taskID, "Failed: ", err.Error())

	stored := engine.tasks[taskID]

	// Add the Task back to delayed or DLQ based on retries.
	if stored.Retries >= stored.MaxRetries {
		log.Println("Task", taskID, "moved to DLQ")
		engine.dlq = append(engine.dlq, task)
		return nil
	}

	backoff := Backoff(50*time.Millisecond, stored.Retries)
	task.RunAt = time.Now().Add(backoff)
	stored.Retries += 1

	log.Println("Task", taskID, "retried!")
	heap.Push(&engine.scheduled, task)

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
func NewEngine() Engine {
	engine := &EngineImpl{
		tasks: make(map[uint32]*TaskMeta),

		ready:     []*Task{},
		completed: []*Task{},
		dlq:       []*Task{},

		processing: make(map[uint32]*Lease),
		scheduled:  ScheduleHeap{},
	}
	heap.Init(&engine.scheduled)

	go engine.Reaper()
	go engine.Scheduler()

	return engine
}
