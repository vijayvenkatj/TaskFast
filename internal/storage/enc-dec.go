package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

func encode(e *LogEntry) ([]byte, error) {

	payload, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}

	total := 4 + len(payload)

	buf := make([]byte, total)

	// write payload length
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))

	// write json payload
	copy(buf[4:], payload)

	return buf, nil
}

func decode(data []byte) (*LogEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid frame")
	}

	payloadLen := binary.LittleEndian.Uint32(data[0:4])

	if len(data) < 4+int(payloadLen) {
		return nil, io.ErrUnexpectedEOF
	}

	payload := data[4 : 4+payloadLen]

	var entry LogEntry

	if err := json.Unmarshal(payload, &entry); err != nil {
		return nil, err
	}

	return &entry, nil
}
