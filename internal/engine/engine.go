package engine

import (
	"container/heap"
	"log"
	"sync"

	"github.com/vijayvenkatj/taskfast/internal/model"
	"github.com/vijayvenkatj/taskfast/internal/storage"
	wal "github.com/vijayvenkatj/taskfast/internal/storage"
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
	event := model.Event{
		Type: model.EnqueueEvent,
		Task: &TaskMeta{
			Task:       task,
			Retries:    0,
			MaxRetries: 1,
		},
		Opts: nil,
		Err:  "",
	}

	err := engine.wal.Append(event)
	if err != nil {
		return err
	}

	_, err = engine.Apply(&event)
	if err != nil {
		return err
	}

	return nil
}

func (engine *EngineImpl) Fetch(opts FetchOptions) *Task {
	event := model.Event{
		Type: model.FetchEvent,
		Task: nil,
		Opts: &opts,
		Err:  "",
	}

	err := engine.wal.Append(event)
	if err != nil {
		return nil
	}

	task, err := engine.Apply(&event)
	if err != nil {
		return nil
	}

	return task
}

func (engine *EngineImpl) Ack(taskID uint32) error {

	event := model.Event{
		Type: model.AckEvent,
		Task: engine.tasks[taskID],
		Opts: nil,
		Err:  "",
	}

	err := engine.wal.Append(event)
	if err != nil {
		return err
	}

	_, err = engine.Apply(&event)
	if err != nil {
		return err
	}

	return nil
}

func (engine *EngineImpl) Fail(taskID uint32, err error) error {

	event := model.Event{
		Type: model.FailEvent,
		Task: engine.tasks[taskID],
		Opts: nil,
		Err:  err.Error(),
	}

	errr := engine.wal.Append(event)
	if errr != nil {
		return errr
	}

	_, errr = engine.Apply(&event)
	if errr != nil {
		return errr
	}

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

	wal, err := wal.NewWAL(logPath)
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
	heap.Init(&engine.scheduled)

	go engine.Reaper()
	go engine.Scheduler()

	return engine
}
