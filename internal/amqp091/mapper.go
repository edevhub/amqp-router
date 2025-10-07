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
	m.logger.Debug("Received method",
		slog.String("method_type", fmt.Sprintf("%T", f.Method)),
		slog.Any("method_attrs", f.Method),
		"channel", f.ChannelId,
		"method_id", f.MethodId,
		"class_id", f.ClassId,
	)
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
		sess, err := m.findSession(f.channel())
		if err != nil {
			return err
		}
		emit <- &transport.Package{Session: sess, Message: &transport.DeclareExchange{
			Name:       method.Exchange,
			Type:       method.Type,
			Durable:    method.Durable,
			AutoDelete: method.AutoDelete,
			Internal:   method.Internal,
			NoWait:     method.NoWait,
			Arguments:  transport.Arguments(method.Arguments),
		}}
		return nil
	case *queueDeclare:
		sess, err := m.findSession(f.channel())
		if err != nil {
			return err
		}
		emit <- &transport.Package{Session: sess, Message: &transport.DeclareQueue{
			Name:       method.Queue,
			Passive:    method.Passive,
			Durable:    method.Exclusive,
			Exclusive:  method.Exclusive,
			AutoDelete: method.AutoDelete,
			NoWait:     method.NoWait,
			Arguments:  transport.Arguments(method.Arguments),
		}}
		return nil
	case *queueBind:
		sess, err := m.findSession(f.channel())
		if err != nil {
			return err
		}
		emit <- &transport.Package{Session: sess, Message: &transport.BindQueue{
			Queue:      method.Queue,
			Exchange:   method.Exchange,
			RoutingKey: method.RoutingKey,
			NoWait:     method.NoWait,
			Arguments:  transport.Arguments(method.Arguments),
		}}
		return nil
	case *basicPublish:
		sess, err := m.findSession(f.channel())
		if err != nil {
			return err
		}
		// TODO: debug why messages are received and mapped empty
		emit <- &transport.Package{Session: sess, Message: &transport.BasicPublish{
			Exchange:   method.Exchange,
			RoutingKey: method.RoutingKey,
			Mandatory:  method.Mandatory,
			Immediate:  method.Immediate,
			Properties: transport.DeliveryProps{
				ContentType:     method.Properties.ContentType,
				ContentEncoding: method.Properties.ContentEncoding,
				Headers:         transport.Arguments(method.Properties.Headers),
				DeliveryMode:    method.Properties.DeliveryMode,
				Priority:        method.Properties.Priority,
				CorrelationId:   method.Properties.CorrelationId,
				ReplyTo:         method.Properties.ReplyTo,
				Expiration:      method.Properties.Expiration,
				MessageId:       method.Properties.MessageId,
				Timestamp:       method.Properties.Timestamp,
				Type:            method.Properties.Type,
				UserId:          method.Properties.UserId,
				AppId:           method.Properties.AppId,
			},
			Body: method.Body,
		}}
		return nil
	default:
		return fmt.Errorf("unsupported method: %T", method)
	}
}

func (m *Mapper) findSession(id uint16) (*transport.Session, error) {
	m.mx.Lock()
	sess, found := m.sessions[id]
	m.mx.Unlock()
	if !found {
		return nil, fmt.Errorf("unknown channel: %d", id)
	}
	return sess, nil
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
