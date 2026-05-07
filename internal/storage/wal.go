package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/vijayvenkatj/taskfast/internal/engine"
)

type LogEntry struct {
	Term     uint32
	LogIndex uint32
	Event    engine.Event
}

type WAL struct {
	mu   sync.Mutex
	file *os.File

	LastIndex   uint32
	CurrentTerm uint32
	Entries     []*LogEntry
}

func NewWAL(filePath string) (*WAL, error) {

	err := os.MkdirAll(filePath, 0755)
	if err != nil {
		return nil, err
	}

	fileName := filepath.Join(filePath, "wal.log")
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

func (wal *WAL) Append(entry *LogEntry) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	data, err := encode(entry)
	if err != nil {
		return err
	}

	n, err := wal.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	if err := wal.file.Sync(); err != nil {
		return err
	}

	wal.Entries = append(wal.Entries, entry)
	wal.LastIndex = entry.LogIndex

	return nil
}

func (wal *WAL) ReadAll() ([]*LogEntry, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.file == nil {
		return nil, nil
	}

	if _, err := wal.file.Seek(0, 0); err != nil {
		return nil, err
	}

	entries := make([]*LogEntry, 0)

	for {
		header := make([]byte, 16)

		_, err := io.ReadFull(wal.file, header)
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}

		eventLen := binary.LittleEndian.Uint32(header[8:12])
		payloadLen := binary.LittleEndian.Uint32(header[12:16])

		totalBody := int(eventLen + payloadLen)
		body := make([]byte, totalBody)

		_, err = io.ReadFull(wal.file, body)
		if err != nil {
			fmt.Println("Read Error:", err)
			break
		}

		data := make([]byte, 16+totalBody)
		copy(data[:16], header)
		copy(data[16:], body)

		entry, err := decode(data)

		entries = append(entries, entry)
	}

	_, err := wal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	wal.Entries = entries

	return entries, nil
}

func (wal *WAL) Get(index uint32) (*LogEntry, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if index == 0 || int(index) > len(wal.Entries) {
		return nil, errors.New("out of bounds")
	}

	return wal.Entries[index-1], nil
}

func (wal *WAL) ReadSince(index uint32) ([]*LogEntry, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if index == 0 {
		return nil, errors.New("out of bounds")
	}
	if int(index) > len(wal.Entries) {
		return []*LogEntry{}, nil
	}

	return wal.Entries[index-1:], nil
}

func (wal *WAL) ReadRange(start, end uint32) ([]*LogEntry, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if start == 0 || end == 0 || int(start) > len(wal.Entries) || int(end) > len(wal.Entries) || start > end {
		return nil, errors.New("out of bounds")
	}

	return wal.Entries[start-1 : end], nil
}

func (wal *WAL) TruncateFrom(start uint32) error {
	if start == 0 || int(start) > len(wal.Entries)+1 {
		return errors.New("out of bounds")
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.Entries = wal.Entries[:start-1]
	wal.LastIndex = start - 1

	if err := wal.file.Truncate(0); err != nil {
		return err
	}
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for _, entry := range wal.Entries {
		data, err := encode(entry)
		if err != nil {
			return err
		}

		n, err := wal.file.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
	}

	return wal.file.Sync()
}

/*
	HELPER FUNCTIONS
*/

// Name returns the file name of the wal.
func (wal *WAL) Name() string {
	return wal.file.Name()
}

// Reset truncates the wal file
func (wal *WAL) Reset() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.file.Truncate(0); err != nil {
		return err
	}
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := wal.file.Sync(); err != nil {
		return err
	}

	return nil
}

// Close closes the wal file.
func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.file != nil {
		return wal.file.Close()
	}

	return nil
}
