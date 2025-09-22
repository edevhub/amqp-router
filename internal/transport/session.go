package transport

import (
	"github.com/google/uuid"
)

type Session struct {
	sid   uuid.UUID
	ID    uint16
	reply chan *Package
}

func OpenSession(id uint16) *Session {
	return &Session{
		ID:    id,
		sid:   uuid.New(),
		reply: make(chan *Package),
	}
}

func (ch *Session) SID() uuid.UUID {
	return ch.sid
}

func (ch *Session) Reply(p *Package) {
	ch.reply <- p
}

func (ch *Session) Receive() <-chan *Package {
	return ch.reply
}

func (ch *Session) Close() {
	close(ch.reply)
}
