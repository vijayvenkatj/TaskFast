package model

type EventType string

const (
	EnqueueEvent = "ENQUEUE"
	FetchEvent   = "FETCH"
	AckEvent     = "ACK"
	FailEvent    = "FAIL"
)

type Event struct {
	Type EventType
	Task *TaskMeta
	Opts *FetchOptions
	Err  string
}
