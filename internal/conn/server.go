package conn

type ServerHandler struct {
	clients map[string]*Connection
	server  *Connection
}
