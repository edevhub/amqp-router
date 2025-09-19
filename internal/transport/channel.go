package transport

import (
	"github.com/google/uuid"
)

type Channel struct {
	uid   uuid.UUID
	ID    uint16
	reply chan *Package
}

func OpenChannel(id uint16) *Channel {
	return &Channel{
		ID:    id,
		uid:   uuid.New(),
		reply: make(chan *Package),
	}
}

func (ch *Channel) UID() uuid.UUID {
	return ch.uid
}

func (ch *Channel) Reply(p *Package) {
	ch.reply <- p
}

func (ch *Channel) Receive() <-chan *Package {
	return ch.reply
}
