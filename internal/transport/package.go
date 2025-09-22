package transport

type Package struct {
	Err     error
	Message any
	Session *Session
}
