package amqp091

import (
	"bufio"
	"io"
)

type (
	Reader = reader
	Writer = writer
)

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (w *Writer) Flush() error {
	if buf, ok := w.w.(*bufio.Writer); ok {
		return buf.Flush()
	}
	return nil
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// spec AMQP091 frames / messages

type (
	Frame               = frame
	HeaderFrame         = headerFrame
	MethodFrame         = methodFrame
	BodyFrame           = bodyFrame
	ProtocolHeaderFrame = protocolHeader

	Message                  = message
	MessageConnectionStart   = connectionStart
	MessageConnectionStartOk = connectionStartOk
	MessageConnectionTune    = connectionTune
	MessageConnectionTuneOk  = connectionTuneOk
	MessageConnectionOpen    = connectionOpen
	MessageConnectionOpenOk  = connectionOpenOk
)
