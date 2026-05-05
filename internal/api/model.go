package api

import (
	"time"

	"github.com/vijayvenkatj/taskfast/internal/engine"
)

type ErrorResp struct {
	Error string `json:"error"`
}

type EnqueueRequest struct {
	Task engine.Task `json:"task"`
}
type EnqueueResponse struct {
	Message string `json:"message"`
}

type FetchRequest struct {
	WorkerID uint32        `json:"worker_id"`
	TaskTime time.Duration `json:"task_time"`
}
type FetchResponse struct {
	Task engine.Task `json:"task"`
}

type DLQRequest struct {
}
type DLQResponse struct {
	DeadTasks []engine.Task `json:"dead_tasks"`
}
