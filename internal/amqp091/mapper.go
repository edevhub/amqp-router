package amqp091

import (
	"fmt"
	"log/slog"

	"github.com/edevhub/amqp-router/internal/transport"
)

type Mapper struct {
	logger         *slog.Logger
	channels       map[uint16]*transport.Channel
	channelsNotify chan *transport.Channel
}

func NewMapper(logger *slog.Logger) *Mapper {
	return &Mapper{
		logger:         logger,
		channels:       make(map[uint16]*transport.Channel),
		channelsNotify: make(chan *transport.Channel),
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
	m.logger.Debug("Received method", slog.Any("method", f.Method), "channel", f.ChannelId, "method_id", f.MethodId, "class_id", f.ClassId)
	switch method := f.Method.(type) {
	case *channelOpen:
		id := f.channel()
		ch := transport.OpenChannel(id)
		m.channels[id] = ch
		go func() {
			m.channelsNotify <- ch
		}()
		ch.Reply(&transport.Package{Message: &transport.Reply{Code: transport.ReplyCodeChannelOpenOk}})
		return nil
	default:
		return fmt.Errorf("unsupported method: %T", method)
	}
}

func (m *Mapper) NotifyNewChannel() <-chan *transport.Channel {
	return m.channelsNotify
}

func (m *Mapper) Cleanup() {
	close(m.channelsNotify)
}
