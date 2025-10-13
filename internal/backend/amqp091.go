package backend

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/edevhub/amqp-router/internal/transport"
	"github.com/google/uuid"
	amqpgo "github.com/rabbitmq/amqp091-go"
)

type AMQP091 struct {
	logger   *slog.Logger
	conn     *amqpgo.Connection
	channels map[uuid.UUID]*amqpgo.Channel

	mx sync.Mutex
}

func NewAMQP091(conf *AMQP091Config, logger *slog.Logger) (*AMQP091, error) {
	conn, err := amqpgo.Dial(conf.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to AMQP: %w", err)
	}

	return &AMQP091{
		conn:     conn,
		logger:   logger,
		channels: make(map[uuid.UUID]*amqpgo.Channel),
	}, nil
}

func (b *AMQP091) Connect(ctx context.Context, in <-chan *transport.Package) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			go b.handlePackage(ctx, <-in)
		}
	}
}

func (b *AMQP091) handlePackage(ctx context.Context, p *transport.Package) {
	ch, err := b.openChannel(p.Session.SID())
	if err != nil {
		// TODO: escalate in case of closed connection, maybe try reconnecting
		b.logger.Error("Failed to open channel", slog.String("sid", p.Session.SID().String()), slog.Any("error", err))
		p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
		return
	}
	switch m := p.Message.(type) {
	case *transport.DeclareExchange:
		if err = ch.ExchangeDeclare(m.Name, m.Type, m.Durable, m.AutoDelete, m.Internal, m.NoWait, castArgumentsToAMQP091Table(m.Arguments)); err != nil {
			p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
			return
		}
		p.Session.Reply(&transport.Package{Message: &transport.Reply{Code: transport.ReplyCodeExchangeDeclareOk}})
	case *transport.DeclareQueue:
		declare := ch.QueueDeclare
		if m.Passive {
			declare = ch.QueueDeclarePassive
		}
		if _, err = declare(m.Name, m.Durable, m.AutoDelete, m.Exclusive, m.NoWait, castArgumentsToAMQP091Table(m.Arguments)); err != nil {
			p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
		}
		p.Session.Reply(&transport.Package{Message: &transport.Reply{Code: transport.ReplyCodeQueueDeclareOk}})
	case *transport.BindQueue:
		if err = ch.QueueBind(m.Queue, m.RoutingKey, m.Exchange, m.NoWait, castArgumentsToAMQP091Table(m.Arguments)); err != nil {
			p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
		}
		p.Session.Reply(&transport.Package{Message: &transport.Reply{Code: transport.ReplyCodeQueueBindOk}})
	case *transport.BasicPublish:
		body, err := io.ReadAll(m.Body)
		if err != nil {
			p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
			return
		}
		confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, m.Exchange, m.RoutingKey, m.Mandatory, m.Immediate, amqpgo.Publishing{
			Headers:         castArgumentsToAMQP091Table(m.Properties.Headers),
			ContentType:     m.Properties.ContentType,
			ContentEncoding: m.Properties.ContentEncoding,
			DeliveryMode:    m.Properties.DeliveryMode,
			Priority:        m.Properties.Priority,
			CorrelationId:   m.Properties.CorrelationId,
			ReplyTo:         m.Properties.ReplyTo,
			Expiration:      m.Properties.Expiration,
			MessageId:       m.Properties.MessageId,
			Timestamp:       m.Properties.Timestamp,
			Type:            m.Properties.Type,
			UserId:          m.Properties.UserId,
			AppId:           m.Properties.AppId,
			Body:            body,
		})
		if err != nil {
			b.logger.Error("Failed to publish message",
				slog.String("sid", p.Session.SID().String()),
				slog.Any("error", err))
			p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
		}
		// TODO: ListenPublish and match with DeliveryTag confirmed to make sure no undelivered closure of the router happen
		if confirm != nil {
			go func(c *amqpgo.DeferredConfirmation, sess *transport.Session) {
				ack, cErr := confirm.WaitContext(ctx)
				if cErr != nil {
					sess.Reply(&transport.Package{Message: &transport.ReplyError{Err: cErr}})
					return
				}
				sess.Reply(&transport.Package{Message: &transport.Reply{Code: transport.ReplyCodeBasicAck, Content: &transport.ReplyConfirmation{
					DeliveryTag: c.DeliveryTag,
					Ack:         ack,
					// TODO: Since we do not listen for frames directly but via amqpgo lib, we cannot send multiple confirms
					Multiple: false,
				}}})
			}(confirm, p.Session)
		}
		// when channel confirmation was not required, no reply on confirmation is expected
		return
	default:
		b.logger.Error("unsupported message", slog.String("sid", p.Session.SID().String()), slog.Any("message", m))
		p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: fmt.Errorf("unsupported message: %T", m)}})
	}
}

func (b *AMQP091) openChannel(sid uuid.UUID) (*amqpgo.Channel, error) {
	b.mx.Lock()
	defer b.mx.Unlock()
	if ch, exist := b.channels[sid]; exist {
		b.logger.Debug("reusing existing channel on backend", slog.String("sid", sid.String()))
		return ch, nil
	}
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, err
	}
	b.logger.Debug("allocated new channel on backend", slog.String("sid", sid.String()))
	b.channels[sid] = ch
	return ch, nil
}

func (b *AMQP091) Close() error {
	return b.conn.Close()
}

type AMQP091Config struct {
	DSN string
}

func castArgumentsToAMQP091Table(in transport.Arguments) amqpgo.Table {
	out := make(amqpgo.Table, len(in))
	for k, v := range in {
		switch val := v.(type) {
		case transport.Arguments:
			out[k] = castArgumentsToAMQP091Table(val)
		case []interface{}:
			arr := make([]interface{}, len(val))
			for i, item := range val {
				if subMap, ok := item.(transport.Arguments); ok {
					arr[i] = castArgumentsToAMQP091Table(subMap)
				} else {
					arr[i] = item
				}
			}
			out[k] = arr
		default:
			out[k] = v
		}
	}
	return out
}
