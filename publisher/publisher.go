package publisher

import (
    "fmt"
    "log"
	"net"
	"net/rpc"

	"github.com/microASO/starter/getter"
)

// Server ...
type Server struct{}

/* getter.ResultSchema
type ResultSchema struct {
	FileID   			string `json:"tm_id"`
	User     			string `json:"tm_username"`
	Role     			string `json:"tm_role"`
	Group    			string `json:"tm_group"`
	Taskname 			string `json:"tm_taskname"`
	Destination     	string `json:"tm_destination"`
	DestinationLfn     	string `json:"tm_destination_lfn"`
	SourceLfn     		string `json:"tm_source_lfn"`
	JobType     		string `json:"tm_type"`
	WorkerName     		string `json:"tm_aso_worker"`
	InputDataset		string `json:"tm_input_dataset"`
	CacheUrl			string `json:"tm_cache_url"`
	DBSUrl 				string `json:"tm_dbs_url"`
	FileSize     		int `json:"tm_filesize"`
	ToPublish     		int `json:"tm_publish"`
}
*/

// Publish ...
func (myself *Server) Publish(payload []getter.ResultSchema, reply *int64) error {

	// get task status

	// if status terminal or len>tot go ahead

	// 	get metadata (getPublDescFiles)

	*reply = 0
	fmt.Println("server task: ", payload[0].Taskname)

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
