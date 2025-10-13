package transport

import (
	"bytes"
	"time"
)

type ReplyCode int

const (
	ReplyCodeChannelOpenOk ReplyCode = iota
	ReplyCodeExchangeDeclareOk
	ReplyCodeQueueDeclareOk
	ReplyCodeQueueBindOk
	ReplyCodeBasicAck
)

type Reply struct {
	Code    ReplyCode
	Content any
}

type ReplyError struct {
	Err error
}

type ReplyConfirmation struct {
	DeliveryTag uint64
	Multiple    bool
	Ack         bool
}

type Arguments map[string]interface{}

type DeclareExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  Arguments
}

type DeclareQueue struct {
	Name       string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  Arguments
}

type BindQueue struct {
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  Arguments
}

type DeliveryProps struct {
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	Headers         Arguments // Application or header exchange table
	DeliveryMode    uint8     // queue implementation use - Transient (1) or Persistent (2)
	Priority        uint8     // queue implementation use - 0 to 9
	CorrelationId   string    // application use - correlation identifier
	ReplyTo         string    // application use - address to to reply to (ex: RPC)
	Expiration      string    // implementation use - message expiration spec
	MessageId       string    // application use - message identifier
	Timestamp       time.Time // application use - message timestamp
	Type            string    // application use - message type name
	UserId          string    // application use - creating user id
	AppId           string    // application use - creating application
}

type BasicPublish struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Properties DeliveryProps
	Body       *bytes.Buffer
}
