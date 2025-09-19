package transport

type ReplyCode int

const (
	ReplyCodeChannelOpenOk ReplyCode = iota
)

type Reply struct {
	Code ReplyCode
}

type Message struct {
}
