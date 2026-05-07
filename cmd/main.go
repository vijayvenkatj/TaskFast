package main

import (
	"log"

	"github.com/vijayvenkatj/taskfast/internal/api"
)

func main() {
	server := api.NewServer(":8080", "./tmp/log")

	log.Println("HTTP server listening on port :8080")
	err := server.ListenAndServe()
	if err != nil {
		return
	}
}
