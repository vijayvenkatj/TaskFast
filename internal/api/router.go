package api

import "net/http"

func NewRouter(handler *Handler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/enqueue", handler.EnqueueTaskHandler)
	mux.HandleFunc("POST /api/fetch", handler.FetchTaskHandler)
	mux.HandleFunc("GET /api/dlq", handler.DLQHandler)

	mux.HandleFunc("POST /api/ack", handler.AckHandler)
	mux.HandleFunc("POST /api/fail", handler.FailHandler)

	return mux
}
