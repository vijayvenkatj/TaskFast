package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/vijayvenkatj/taskfast/internal/engine"
)

type LogEntry struct {
	Event engine.Event `json:"event"`
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWAL(path string) (*WAL, error) {

	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

func (w *WAL) Append(evt engine.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := LogEntry{
		Event: evt,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	// write frame length
	err = binary.Write(w.file, binary.LittleEndian, uint32(len(data)))
	if err != nil {
		return err
	}

	// write payload
	n, err := w.file.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return io.ErrShortWrite
	}

	// force durability
	if err := w.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (w *WAL) Replay() ([]engine.Event, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	events := make([]engine.Event, 0)

	for {
		var length uint32

		err := binary.Read(w.file, binary.LittleEndian, &length)
		if err == io.EOF {
			break
		}

		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		payload := make([]byte, length)

		_, err = io.ReadFull(w.file, payload)

		if err == io.EOF {
			break
		}

		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		var entry LogEntry

		if err := json.Unmarshal(payload, &entry); err != nil {
			return nil, err
		}

		events = append(events, entry.Event)
	}

	// move cursor back to end for future appends
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return events, nil
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	return w.file.Close()
}

func (w *WAL) Name() string {
	return w.path
}
