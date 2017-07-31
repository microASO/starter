package publisher

import (
	"log"
	"net"
	"net/http"
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

// PubServer ...
func PubServer(logger *log.Logger) {
	rpc.Register(new(Server))
	rpc.HandleHTTP()
	session, err := net.Listen("tcp", "127.0.0.1:3126")
	if err != nil {
		logger.Println("error: ", err)
		return
	}
	for {
		go http.Serve(session, nil)
	}

}
