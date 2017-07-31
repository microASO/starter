package publisher

import (
	"net/rpc"

	"github.com/microASO/starter/getter"
)

// Server ...
type Server struct{}

// Publish ...
func (myself *Server) Publish(payload []getter.ResultSchema, reply *int64) error {
	*reply = 0
	return nil
}

func PubServer() {
	rpc.Register(new(Server))

}
