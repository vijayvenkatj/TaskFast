package api

import "net/http"

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (handler *Handler) EnqueueTaskHandler(w http.ResponseWriter, r *http.Request) {}
func (handler *Handler) FetchTaskHandler(w http.ResponseWriter, r *http.Request)   {}
