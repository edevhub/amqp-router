package transport

import (
	"fmt"

	"github.com/google/uuid"
)

type Session struct {
	sid   uuid.UUID
	ID    uint16
	pub   *PublishCollector
	reply chan *Package
}

func OpenSession(id uint16) *Session {
	return &Session{
		ID:    id,
		sid:   uuid.New(),
		reply: make(chan *Package),
	}
}

func (s *Session) SID() uuid.UUID {
	return s.sid
}

func (s *Session) StartPublishing(msg *BasicPublish) error {
	if s.pub != nil {
		return fmt.Errorf("pending publishing already exists")
	}
	s.pub = &PublishCollector{msg: msg}
	return nil
}

func (s *Session) PublishCollector() *PublishCollector {
	return s.pub
}

func (s *Session) FlushPublishing() {
	s.pub = nil
}

func (s *Session) Reply(p *Package) {
	s.reply <- p
}

func (s *Session) Receive() <-chan *Package {
	return s.reply
}

func (s *Session) Close() {
	close(s.reply)
}

type PublishCollector struct {
	Size uint64
	msg  *BasicPublish
}

func (c *PublishCollector) Message() *BasicPublish {
	return c.msg
}
