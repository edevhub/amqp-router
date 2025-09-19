package transport

type Package struct {
	Err     error
	Message any
	Channel *Channel
}
