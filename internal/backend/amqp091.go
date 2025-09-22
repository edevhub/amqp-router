package backend

import (
	"context"
	"fmt"
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

func (b *AMQP091) handlePackage(_ context.Context, p *transport.Package) {
	ch, err := b.openChannel(p.Session.SID())
	if err != nil {
		// TODO: escalate in case of closed connection, maybe try reconnecting
		b.logger.Error("Failed to open channel", slog.String("sid", p.Session.SID().String()), slog.Any("error", err))
		p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
		return
	}
	b.logger.Debug("allocated channel on backend", slog.String("sid", p.Session.SID().String()))
	switch m := p.Message.(type) {
	case *transport.DeclareExchange:
		if err = ch.ExchangeDeclare(m.Name, m.Type, m.Durable, m.AutoDelete, m.Internal, m.NoWait, m.Arguments); err != nil {
			p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: err}})
			return
		}
		p.Session.Reply(&transport.Package{Message: &transport.Reply{Code: transport.ReplyCodeExchangeDeclareOk}})
	default:
		b.logger.Error("unsupported message", slog.String("sid", p.Session.SID().String()), slog.Any("message", m))
		p.Session.Reply(&transport.Package{Message: &transport.ReplyError{Err: fmt.Errorf("unsupported message: %T", m)}})
	}
}

func (b *AMQP091) openChannel(sid uuid.UUID) (*amqpgo.Channel, error) {
	b.mx.Lock()
	defer b.mx.Unlock()
	if ch, exist := b.channels[sid]; exist {
		return ch, nil
	}
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, err
	}
	b.channels[sid] = ch
	return ch, nil
}

func (b *AMQP091) Close() error {
	return b.conn.Close()
}

type AMQP091Config struct {
	DSN string
}
