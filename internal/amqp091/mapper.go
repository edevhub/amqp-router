package amqp091

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/edevhub/amqp-router/internal/transport"
	"github.com/google/uuid"
)

type Mapper struct {
	logger         *slog.Logger
	sessions       map[uint16]*transport.Session
	sessionsBySID  map[uuid.UUID]uint16
	sessionsNotify chan *transport.Session

	mx sync.Mutex
}

func NewMapper(logger *slog.Logger) *Mapper {
	return &Mapper{
		logger:         logger,
		sessions:       make(map[uint16]*transport.Session),
		sessionsBySID:  make(map[uuid.UUID]uint16),
		sessionsNotify: make(chan *transport.Session),
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
		m.mx.Lock()
		ch := transport.OpenSession(id)
		m.sessions[id] = ch
		m.sessionsBySID[ch.SID()] = id
		m.mx.Unlock()
		go func() {
			m.sessionsNotify <- ch
		}()
		ch.Reply(&transport.Package{Message: &transport.Reply{Code: transport.ReplyCodeChannelOpenOk}})
		return nil
	case *exchangeDeclare:
		id := f.channel()
		m.mx.Lock()
		ch, found := m.sessions[id]
		m.mx.Unlock()
		if !found {
			return fmt.Errorf("unknown channel: %d", id)
		}
		emit <- &transport.Package{Session: ch, Message: &transport.DeclareExchange{
			Name:       method.Exchange,
			Type:       method.Type,
			Durable:    method.Durable,
			AutoDelete: method.AutoDelete,
			Internal:   method.Internal,
			NoWait:     method.NoWait,
			Arguments:  method.Arguments,
		}}
		return nil
	default:
		return fmt.Errorf("unsupported method: %T", method)
	}
}

func (m *Mapper) NotifyNewSession() <-chan *transport.Session {
	return m.sessionsNotify
}

func (m *Mapper) RecycleSession(sid uuid.UUID) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if id, found := m.sessionsBySID[sid]; found {
		if s, found := m.sessions[id]; found {
			delete(m.sessions, id)
			delete(m.sessionsBySID, sid)
			s.Close()
		}
	}
}

func (m *Mapper) Cleanup() {
	for sid := range m.sessionsBySID {
		m.RecycleSession(sid)
	}
	close(m.sessionsNotify)
}
