package api

import "net/http"

func NewRouter(handler *Handler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/enqueue", handler.EnqueueTaskHandler)
	mux.HandleFunc("POST /api/fetch", handler.FetchTaskHandler)

	return mux
}
