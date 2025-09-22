package transport

type ReplyCode int

const (
	ReplyCodeChannelOpenOk ReplyCode = iota
	ReplyCodeExchangeDeclareOk
)

type Reply struct {
	Code ReplyCode
}

type ReplyError struct {
	Err error
}

type DeclareExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  map[string]interface{}
}
