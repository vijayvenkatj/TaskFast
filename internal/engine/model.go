package engine

import (
	"time"

	"github.com/vijayvenkatj/taskfast/internal/model"
)

// Task - User provided JOB
type Task = model.Task
type TaskMeta = model.TaskMeta
type FetchOptions = model.FetchOptions

type Lease struct {
	WorkerID   uint32
	TaskID     uint32
	LeaseUntil time.Time
}
