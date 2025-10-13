package amqp091

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
		m.logger.Debug("Received heartbeat frame", slog.Any("frame", fr))
		return nil
	case *methodFrame:
		return m.handleMethod(fr, emit)
	case *headerFrame:
		return m.handleHeader(fr)
	case *bodyFrame:
		return m.handleBody(fr, emit)
	default:
		m.logger.Error("Unsupported frame type", slog.Any("frame", f), slog.String(fmt.Sprintf("%T", f), fmt.Sprintf("%T", f)))
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
		args, err := castArguments(method.Arguments)
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
			Arguments:  args,
		}}
		return nil
	case *queueDeclare:
		sess, err := m.findSession(f.channel())
		if err != nil {
			return err
		}
		args, err := castArguments(method.Arguments)
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
			Arguments:  args,
		}}
		return nil
	case *queueBind:
		sess, err := m.findSession(f.channel())
		if err != nil {
			return err
		}
		args, err := castArguments(method.Arguments)
		if err != nil {
			return err
		}
		emit <- &transport.Package{Session: sess, Message: &transport.BindQueue{
			Queue:      method.Queue,
			Exchange:   method.Exchange,
			RoutingKey: method.RoutingKey,
			NoWait:     method.NoWait,
			Arguments:  args,
		}}
		return nil
	case *basicPublish:
		sess, err := m.findSession(f.channel())
		if err != nil {
			return err
		}
		if err = sess.StartPublishing(&transport.BasicPublish{
			Exchange:   method.Exchange,
			RoutingKey: method.RoutingKey,
			Mandatory:  method.Mandatory,
			Immediate:  method.Immediate,
			Body:       bytes.NewBuffer(nil),
		}); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported method: %T", method)
	}
}

func (m *Mapper) handleHeader(f *headerFrame) error {
	sess, err := m.findSession(f.channel())
	if err != nil {
		return err
	}
	pc := sess.PublishCollector()
	if pc == nil {
		return fmt.Errorf("no message collected for publishing at channel: %d (%s)", f.channel(), sess.SID().String())
	}
	pc.Size = f.Size
	headers, err := castArguments(f.Properties.Headers)
	pc.Message().Properties = transport.DeliveryProps{
		ContentType:     f.Properties.ContentType,
		ContentEncoding: f.Properties.ContentEncoding,
		Headers:         headers,
		DeliveryMode:    f.Properties.DeliveryMode,
		Priority:        f.Properties.Priority,
		CorrelationId:   f.Properties.CorrelationId,
		ReplyTo:         f.Properties.ReplyTo,
		Expiration:      f.Properties.Expiration,
		MessageId:       f.Properties.MessageId,
		Timestamp:       f.Properties.Timestamp,
		Type:            f.Properties.Type,
		UserId:          f.Properties.UserId,
		AppId:           f.Properties.AppId,
	}
	return nil
}

func (m *Mapper) handleBody(f *bodyFrame, emit chan<- *transport.Package) error {
	sess, err := m.findSession(f.channel())
	if err != nil {
		return err
	}
	pc := sess.PublishCollector()
	if pc == nil {
		return fmt.Errorf("no message collected for publishing at channel: %d (%s)", f.channel(), sess.SID().String())
	}
	pc.Message().Body.Write(f.Body)
	if uint64(pc.Message().Body.Len()) == pc.Size {
		emit <- &transport.Package{Session: sess, Message: pc.Message()}
		sess.FlushPublishing()
	}
	return nil
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

func castArguments(in Table) (transport.Arguments, error) {
	out := make(transport.Arguments, len(in))
	for k, v := range in {
		switch vt := v.(type) {
		case nil, bool, byte, int8, int, int16, int32, int64, float32, float64, string, []byte, Decimal, time.Time:
			out[k] = vt
		case Table:
			nested, err := castArguments(vt)
			if err != nil {
				return nil, fmt.Errorf("nested table %q: %w", k, err)
			}
			out[k] = nested
		case []interface{}:
			arr := make([]interface{}, len(vt))
			for i, elem := range vt {
				switch et := elem.(type) {
				case bool, int, int16, int32, int64, float32, float64, string, []byte, time.Time, nil:
					arr[i] = et
				case Table:
					nested, err := castArguments(et)
					if err != nil {
						return nil, fmt.Errorf("array[%d] nested table: %w", i, err)
					}
					arr[i] = nested
				default:
					return nil, fmt.Errorf("unsupported array element type at index %d: %T", i, elem)
				}
			}
			out[k] = arr
		default:
			return nil, fmt.Errorf("unsupported argument type: %T", v)
		}
	}
	return out, nil
}
