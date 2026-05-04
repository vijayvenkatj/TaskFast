package api

import (
	"errors"
	"log"
	"net/http"
)

type Server struct {
	HttpServer *http.Server
}

func NewServer(addr string) *Server {

	handler := NewHandler()
	router := NewRouter(handler)

	return &Server{
		HttpServer: &http.Server{
			Addr:    addr,
			Handler: router,
		},
	}
}

func (server *Server) ListenAndServe() error {
	if err := server.HttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %s\n", err)
		return err
	}
	return nil
}
