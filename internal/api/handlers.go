package api

import (
	"encoding/json"
	"net/http"

	"github.com/vijayvenkatj/taskfast/internal/engine"
)

type Handler struct {
	Engine engine.Engine
}

func NewHandler(engine engine.Engine) *Handler {
	return &Handler{
		Engine: engine,
	}
}

func (handler *Handler) EnqueueTaskHandler(w http.ResponseWriter, r *http.Request) {

	var request EnqueueRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResp{Error: "bad request"})
		return
	}

	err = handler.Engine.Enqueue(&request.Task)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResp{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, EnqueueResponse{Message: "task enqueued"})
}

func (handler *Handler) FetchTaskHandler(w http.ResponseWriter, r *http.Request) {

	var request FetchRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResp{Error: "bad request"})
		return
	}

	fetchOptions := engine.FetchOptions{
		WorkerID: request.WorkerID,
		TaskTime: request.TaskTime,
	}

	task := handler.Engine.Fetch(fetchOptions)

	writeJSON(w, http.StatusCreated, FetchResponse{*task})
}

func (handler *Handler) DLQHandler(w http.ResponseWriter, r *http.Request) {
	dead_tasks := handler.Engine.DLQ()
	writeJSON(w, http.StatusAccepted, DLQResponse{
		DeadTasks: dead_tasks,
	})
}

// Helpers

func writeJSON(w http.ResponseWriter, status int, resp any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}
