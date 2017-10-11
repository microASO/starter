package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/url"
	"os"
	"time"

	"github.com/microASO/starter/getter"
)

type configuration struct {
	Proxy   string
	LogPath string
}

/* // Get config from file
type configuration struct {
	Proxy   string `json:"proxy"`
	LogPath string `json:"logPath"`
}*/

func pubServer(logger *log.Logger) {
	serv := new(getter.Server)
	rpc.Register(serv)

	logger.Print("Starting publisher...")
	session, err := net.Listen("tcp", "0.0.0.0:8446")
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

func main() {
	// get configuration
	config := configuration{Proxy: "/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy", LogPath: "stdout"}

	/*  // Get config from file
	configPath := "src/github.com/microASO/starter/config/conf.json"
	absPath, _ := filepath.Abs(configPath)
	file, err := os.Open(absPath)
	if err != nil {
		fmt.Println("error: ", err)
	}
	decoder := json.NewDecoder(file)
	configuration := configuration{}
	err = decoder.Decode(&configuration)
	if err != nil {
		fmt.Println("error:", err)
	}
	*/

	// check for command line parameter, but keep the conf.json as the default
	var (
		certFile = flag.String("cert", config.Proxy, "A PEM eoncoded certificate file.")
		keyFile  = flag.String("key", config.Proxy, "A PEM encoded private key file.")
		logFile  = flag.String("out", config.LogPath, "Redirect output to this file. Default stdout")
	)

	flag.Parse()
	var logger *log.Logger

	if *logFile != "stdout" {
		logfile, err := os.OpenFile(*logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			fmt.Println("error: ", err)
		}
		defer logfile.Close()
		logger = log.New(logfile, "Starter ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
		fmt.Printf("Redirecting output to %s \n", *logFile)
	} else {
		logger = log.New(os.Stdout, "Starter ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	}

	// Start the process
	logger.Print("Processing Started\n")
	logger.Printf("Using certfile: %s and keyfile: %s \n", *certFile, *keyFile)

	// start publisher server
	go pubServer(logger)
	// main loop
	for {
		// define query endpoint
		reqURL := "https://vocms035.cern.ch/crabserver/dev/filetransfers"
		data := url.Values{"subresource": {"acquirePublication"}, "asoworker": {"asodciangot1"}}.Encode()
		//data := `{"subresource": "acquirePublication", "asoworker": "asodciangot1"}`

		// make request
		// acquire users to publish
		logger.Print("Binding publication to this instance: ", data)
		respo, err := getter.RequestHandler(reqURL, data, "POST", *certFile, *keyFile)
		if err != nil {
			logger.Printf("Error retrieving publication with %s", reqURL+"?"+data)
			logger.Fatal(err)
		}
		logger.Print("Publications acquired: %s", respo)

		// get publications
		data = url.Values{"subresource": {"acquiredPublication"},
			"asoworker": {"asodciangot1"},
			"username":  {"dciangot"},
			"grouping":  {"1"},
			"limit":     {"10000"}}.Encode()
		//data = "subresource=acquiredPublication&asoworker=asoprod1&grouping=0&limit=1000"

		log.Print("Getting publications bound to this instance")
		response, err := getter.RequestHandler(reqURL, "?"+data, "GET", *certFile, *keyFile)
		if err != nil {
			logger.Printf("Error retrieving publication with %s", reqURL+"?"+data)
			logger.Fatal(err)
		}
		logger.Print("Got publications, processing tasks... %s ? %s ", reqURL, data)
		logger.Print("Got publications, processing tasks... %s", response)

		responseBYTE := []byte(response)
		var responseJSON getter.RestOutput
		json.Unmarshal(responseBYTE, &responseJSON)

		// buffer users
		ch := make(chan []getter.ResultSchema, 100)

		// buffer for splitting
		go getter.SplitFiles(responseJSON, ch, logger)

		// got tasks/files per user, then send
		reqURL = "0.0.0.0:8446"
		for i := 0; i < 10; i++ {
			go getter.SendTask(ch, reqURL, logger)
		}
		time.Sleep(time.Second * 600)
	}
}
