package conn

import (
	"fmt"
	"github.com/edevhub/amqp-router/internal/amqp091"
	"log"
	"net"
	"reflect"
	"sync"
)

type (
	Connection struct {
		w    *amqp091.Writer
		r    *amqp091.Reader
		conn net.Conn

		sendMx sync.Mutex
	}
	Package struct {
		Frame amqp091.Frame
		Err   error
	}
)

func NewConnection(c net.Conn) *Connection {
	return &Connection{
		w:    amqp091.NewWriter(c),
		r:    amqp091.NewReader(c),
		conn: c,
	}
}

func (c *Connection) Send(frames ...amqp091.Frame) error {
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

func (c *Connection) Init() (<-chan *Package, error) {
	log.Printf("Sending protocol header")
	hs, err := amqp091.ReadProtocolHeader(c.conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read protocol header: %w", err)
	}
	if !amqp091.ValidateProtocolHeader(hs) {
		return nil, fmt.Errorf("invalid protocol header: %v", hs)
	}

	if err = c.openConnection(); err != nil {
		return nil, fmt.Errorf("open connection sequence failed: %w", err)
	}

	out := make(chan *Package)
	go func() {
		defer close(out)
		for {
			frame, err := c.r.ReadFrame()
			out <- &Package{
				Frame: frame,
				Err:   err,
			}
		}
	}()

	return out, nil
}

func (c *Connection) Close() error {
	if err := c.w.Flush(); err != nil {
		return err
	}
	return c.conn.Close()
}

func (c *Connection) openConnection() error {
	fstart := &amqp091.MessageConnectionStart{
		VersionMajor: 0,
		VersionMinor: 9,
		ServerProperties: amqp091.Table{
			"product":  "RMQRouter",
			"version":  "0.1.0",
			"platform": "golang1.24",
		},
		Mechanisms: "PLAIN AMQPLAIN",
		Locales:    "en_US",
	}
	fstartOk := &amqp091.MessageConnectionStartOk{}
	log.Printf("Sending AMQP connection start")
	if err := c.call(fstart, fstartOk); err != nil {
		return fmt.Errorf("Connection.Start sequence failure: %w", err)
	}

	ftune := &amqp091.MessageConnectionTune{
		ChannelMax: 8,      // Use the minimum channel size among all backends
		FrameMax:   131072, // Use the minimum frame size among all backends
		Heartbeat:  300,    // Use the minimum heartbeat among all backends
	}
	ftuneOk := &amqp091.MessageConnectionTuneOk{}
	fopen := &amqp091.MessageConnectionOpen{}
	log.Printf("Sending AMQP connection tune")
	if err := c.call(ftune, ftuneOk, fopen); err != nil {
		return fmt.Errorf("Connection.Tune sequence failure: %w", err)
	}

	fopenOk := &amqp091.MessageConnectionOpenOk{}
	log.Printf("Confirming AMQP connection open")
	if err := c.call(fopenOk); err != nil {
		return fmt.Errorf("Connection.Open sequence failure: %w", err)
	}
	return nil
}

func (c *Connection) call(req amqp091.Message, res ...amqp091.Message) error {
	// req is nil if we don't need to send a request, but we still need to read incoming frames
	if req != nil {
		if err := c.Send(&amqp091.MethodFrame{ChannelId: 0, Method: req}); err != nil {
			return err
		}
	}

	for _, try := range res {
		f, err := c.r.ReadFrame()
		if err != nil {
			return fmt.Errorf("failed to read frame: %w", err)
		}
		mf, ok := f.(*amqp091.MethodFrame)
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
