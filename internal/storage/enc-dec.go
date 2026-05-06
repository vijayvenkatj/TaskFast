package wal

import (
	"encoding/binary"
	"fmt"
)

func encode(e *LogEntry) []byte {

	event := []byte(e.Event)
	payload := e.EventPayload

	total := 16 + len(event) + len(payload)
	buf := make([]byte, total)

	binary.LittleEndian.PutUint32(buf[0:4], e.Term)
	binary.LittleEndian.PutUint32(buf[4:8], e.LogIndex)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(event)))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(payload)))

	offset := 16
	copy(buf[offset:], event)
	offset += len(event)

	copy(buf[offset:], payload)
	offset += len(payload)

	return buf
}

func decode(data []byte) (*LogEntry, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("invalid header")
	}

	offset := 0

	term := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	idx := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	eventLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	payloadLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	total := int(eventLen + payloadLen)
	if len(data) < 16+total {
		return nil, fmt.Errorf("incomplete entry")
	}

	event := string(data[offset : offset+int(eventLen)])
	offset += int(eventLen)

	payload := data[offset : offset+int(payloadLen)]
	offset += int(payloadLen)

	return &LogEntry{
		Term:         term,
		LogIndex:     idx,
		Event:        event,
		EventPayload: payload,
	}, nil
}
