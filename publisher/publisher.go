package publisher

import (
	"log"
	"net"
	"net/rpc"

	"github.com/microASO/starter/getter"
)

// Server ...
type Server struct{}

// Publish ...
func (myself *Server) Publish(payload []getter.ResultSchema, reply *int64, logger *log.Logger) error {

	*reply = 0
	logger.Println("server task: ", payload[0].Taskname)

	return nil
}

// PubServer ...
func PubServer(logger *log.Logger) {
	serv := new(Server)
	rpc.Register(serv)

	logger.Print("Starting publisher...")
	session, err := net.Listen("tcp", "127.0.0.1:3126")
	if err != nil {
		logger.Println("error: ", err)
		return
	}

	for {
		conn, err := session.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}

}
