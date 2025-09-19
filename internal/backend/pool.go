package backend

import (
	"fmt"
	"log/slog"
	"sync"
)

const Default = "default"

type Pool struct {
	conn    map[string]*AMQP091
	configs map[string]*AMQP091Config
	logger  *slog.Logger
	mx      sync.Mutex
}

func NewPool(configs map[string]*AMQP091Config, logger *slog.Logger) *Pool {
	return &Pool{
		conn:    make(map[string]*AMQP091),
		configs: configs,
		logger:  logger,
	}
}

func (p *Pool) GetDefault() (*AMQP091, error) {
	return p.Get(Default)
}

func (p *Pool) Get(name string) (*AMQP091, error) {
	p.mx.Lock()
	defer p.mx.Unlock()

	if c, exist := p.conn[name]; exist {
		c.logger.Debug("reusing connected backend", slog.String("backend", name))
		return c, nil
	}
	cfg, exist := p.configs[name]
	if !exist {
		return nil, fmt.Errorf("backend [%s] not found", name)
	}
	c, err := NewAMQP091(cfg, p.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create backend [%s]: %w", name, err)
	}
	c.logger.Debug("connected to backend", slog.String("backend", name))
	p.conn[name] = c
	return c, nil
}

func (p *Pool) Close() {
	p.mx.Lock()
	defer p.mx.Unlock()
	for name, c := range p.conn {
		if err := c.Close(); err != nil {
			p.logger.Error("Error closing AMQP connection", slog.Any("error", err), slog.String("backend", name))
		}
	}
}
