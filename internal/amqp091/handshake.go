package amqp091

import (
	"bytes"
	"io"
)

var ProtocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

func validateProtocolHeader(header []byte) bool {
	return bytes.Equal(header, ProtocolHeader)
}

func readProtocolHeader(r io.Reader) ([]byte, error) {
	header := make([]byte, len(ProtocolHeader))
	_, err := r.Read(header)
	if err != nil {
		return nil, err
	}
	return header, nil
}
