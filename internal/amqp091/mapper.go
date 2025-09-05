package amqp091

import (
	"fmt"

	"github.com/edevhub/amqp-router/internal/transport"
)

type Mapper struct {
	channels map[uint16]*transport.Channel
}

func NewMapper() *Mapper {
	return &Mapper{
		channels: make(map[uint16]*transport.Channel),
	}
}

func (m *Mapper) MapFrame(f frame, emit chan<- *transport.Package) error {
	switch fr := f.(type) {
	case *heartbeatFrame:
		// TODO: handle heartbeat
		return nil
	case *methodFrame:
		return m.handleMethod(fr, emit)
	}
	return nil
}

func (m *Mapper) handleMethod(f *methodFrame, emit chan<- *transport.Package) error {
	switch method := f.Method.(type) {
	case *channelOpen:
		id := f.channel()
		ch := &transport.Channel{ID: id}
		m.channels[id] = ch
		emit <- &transport.Package{Payload: ch}
		return nil
	default:
		return fmt.Errorf("unsupported method: %T", method)
	}
}

func (m *Mapper) Cleanup() {}
