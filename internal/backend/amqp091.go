package backend

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/edevhub/amqp-router/internal/transport"
	"github.com/google/uuid"
	amqpgo "github.com/rabbitmq/amqp091-go"
)

type AMQP091 struct {
	logger   *slog.Logger
	conn     *amqpgo.Connection
	channels map[uuid.UUID]*amqpgo.Channel
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
			p := <-in
			_, err := b.openChannel(p.Channel.UID())
			if err != nil {
				return fmt.Errorf("failed to open channel: %w", err)
			}
			b.logger.Debug("allocated channel on backend")
		}
	}
}

func (b *AMQP091) openChannel(uid uuid.UUID) (*amqpgo.Channel, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, err
	}
	b.channels[uid] = ch
	return ch, nil
}

func (b *AMQP091) Close() error {
	return b.conn.Close()
}

type AMQP091Config struct {
	DSN string
}
