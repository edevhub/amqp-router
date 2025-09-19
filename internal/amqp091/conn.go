package amqp091

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"

	"github.com/edevhub/amqp-router/internal/transport"
)

type (
	Connection struct {
		w      *writer
		r      *reader
		conn   net.Conn
		logger *slog.Logger

		sendMx sync.Mutex
	}
)

func NewConnection(c net.Conn, l *slog.Logger) *Connection {
	return &Connection{
		w:      &writer{w: c},
		r:      &reader{r: c},
		conn:   c,
		logger: l,
	}
}

func (c *Connection) Handle(ctx context.Context) (<-chan *transport.Package, error) {
	l := c.logger.With(
		slog.String("remote", c.conn.RemoteAddr().String()),
		slog.String("local", c.conn.LocalAddr().String()),
	)
	defer func() {
		if err := c.Close(); err != nil {
			l.Error("Error closing AMQP connection", slog.Any("error", err))
		}
	}()

	// establishing connection
	if err := c.init(); err != nil {
		return nil, fmt.Errorf("failed to init AMQP connection: %w", err)
	}

	out := make(chan *transport.Package)
	mapper := NewMapper(c.logger)
	go c.handleReplies(ctx, mapper.NotifyNewChannel())
	go func() {
		defer close(out)
		defer mapper.Cleanup()
		for {
			select {
			case <-ctx.Done():
				// TODO: handle context cancellation
				return
			default:
				f, err := c.r.ReadFrame()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return
					}
					out <- &transport.Package{Err: err}
					return
				}
				if err = mapper.MapFrame(f, out); err != nil {
					c.logger.Error("Failed to map frame", slog.Any("error", err), slog.Any("frame", f))
					out <- &transport.Package{Err: fmt.Errorf("unexpected frame: %w", err)}
					return
				}
			}
		}
	}()

	return out, nil
}

func (c *Connection) send(frames ...frame) error {
	if len(frames) == 0 {
		return nil
	}

	c.sendMx.Lock()
	defer c.sendMx.Unlock()

	if len(frames) == 1 {
		return c.w.WriteFrame(frames[0])
	}

	for _, frame := range frames {
		if err := c.w.WriteFrameNoFlush(frame); err != nil {
			return err
		}
	}
	return c.w.Flush()
}

func (c *Connection) init() error {
	c.logger.Debug("Sending protocol header")
	hs, err := readProtocolHeader(c.conn)
	if err != nil {
		return fmt.Errorf("failed to read protocol header: %w", err)
	}
	if !validateProtocolHeader(hs) {
		return fmt.Errorf("invalid protocol header: %v", hs)
	}

	if err = c.openConnection(); err != nil {
		return fmt.Errorf("open connection sequence failed: %w", err)
	}
	return nil
}

func (c *Connection) Close() error {
	if err := c.w.Flush(); err != nil {
		return err
	}
	return nil
}

func (c *Connection) openConnection() error {
	fstart := &connectionStart{
		VersionMajor: 0,
		VersionMinor: 9,
		ServerProperties: Table{
			"product":  "AMQPRouter",
			"version":  "0.1.0",
			"platform": "golang1.25",
		},
		Mechanisms: "PLAIN AMQPLAIN",
		Locales:    "en_US",
	}
	fstartOk := &connectionStartOk{}
	c.logger.Debug("Sending AMQP connection start")
	if err := c.call(fstart, fstartOk); err != nil {
		return fmt.Errorf("Connection.Start sequence failure: %w", err)
	}

	// TODO: handle secure frame

	// TODO: handle defaults
	ftune := &connectionTune{
		ChannelMax: 32,
		FrameMax:   131072,
		Heartbeat:  300,
	}
	ftuneOk := &connectionTuneOk{}
	fopen := &connectionOpen{}
	c.logger.Debug("Sending AMQP connection tune")
	if err := c.call(ftune, ftuneOk, fopen); err != nil {
		return fmt.Errorf("Connection.Tune sequence failure: %w", err)
	}

	fopenOk := &connectionOpenOk{}
	c.logger.Debug("Sending AMQP connection open")
	if err := c.call(fopenOk); err != nil {
		return fmt.Errorf("Connection.Open sequence failure: %w", err)
	}
	return nil
}

// TODO: refactor to get rid of reflection (maybe replace with switch)
func (c *Connection) call(req message, expect ...message) error {
	// req is nil if we don't need to send a request, but we still need to read incoming frames
	if req != nil {
		if err := c.send(&methodFrame{ChannelId: 0, Method: req}); err != nil {
			return err
		}
	}

	for _, try := range expect {
		f, err := c.r.ReadFrame()
		if err != nil {
			return fmt.Errorf("failed to read frame: %w", err)
		}
		mf, ok := f.(*methodFrame)
		if !ok {
			return fmt.Errorf("expected method frame, got %T", f)
		}
		if reflect.TypeOf(try) != reflect.TypeOf(mf.Method) {
			return fmt.Errorf("expected a message type(%T), got %T", try, mf.Method)
		}
		vres := reflect.ValueOf(try).Elem()
		vmsg := reflect.ValueOf(mf.Method).Elem()
		vres.Set(vmsg)
	}

	return nil
}

func (c *Connection) handleReplies(ctx context.Context, notify <-chan *transport.Channel) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("Context cancelled, stop handling new reply channels")
			return
		case ch := <-notify:
			c.listenReplies(ctx, ch)
		}
	}
}

func (c *Connection) listenReplies(ctx context.Context, ch *transport.Channel) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("Context cancelled, stop listening for replies on channel", slog.Int("channel", int(ch.ID)))
			return
		case p := <-ch.Receive():
			c.logger.Debug("Received reply", slog.Any("reply", p))
			switch m := p.Message.(type) {
			case *transport.Reply:
				c.logger.Debug("Sending reply message", slog.Any("reply", m))
				if err := c.sendReplyMessage(ch, m); err != nil {
					c.logger.Error("Failed to send reply message", slog.Any("error", err), slog.Any("reply", m))
				}
			}
		}
	}
}

func (c *Connection) sendReplyMessage(ch *transport.Channel, m *transport.Reply) error {
	switch m.Code {
	case transport.ReplyCodeChannelOpenOk:
		c.logger.Debug("Sending channel open ok reply")
		return c.send(&methodFrame{
			ChannelId: ch.ID,
			Method:    &channelOpenOk{},
		})
	default:
		c.logger.Debug("cannot handle reply message", slog.Any("reply_code", m.Code))
	}
	return nil
}
