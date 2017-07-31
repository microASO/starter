package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/microASO/starter/getter"
)

var (
	certFile = flag.String("cert", "usercert.pem", "A PEM eoncoded certificate file.")
	keyFile  = flag.String("key", "unencrypted.pem", "A PEM encoded private key file.")
)

func main() {
	logfile, err := os.OpenFile("mylog", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println(err)
	}
	//defer to close when you're done with it, not because you think it's idiomatic!
	defer logfile.Close()
	//logger := log.New(logfile, "Starter ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	logger := log.New(os.Stdout, "Starter ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

	logger.Print("Processing Started\n")
	flag.Parse()
	logger.Printf("Using certfile: %s and keyfile: %s", *certFile, *keyFile)

	// define query endpoint
	url := "https://cmsweb-testbed.cern.ch/crabserver/preprod/filetransfers"
	data := "subresource=acquirePublication&asoworker=asoprod1"

	// make request
	// acquire users to publish
	logger.Print("Binding publication to this instance")
	_, err = getter.RequestHandler(url, "?"+data, "GET", *certFile, *keyFile)
	if err != nil {
		logger.Printf("Error retrieving publication with %s", url+"?"+data)
		logger.Fatal(err)
	}
	logger.Print("Publications acquired")

	// get publications
	data = "subresource=acquiredPublication&asoworker=asoprod1&grouping=0&limit=1000"
	log.Print("Getting publications bound to this instance")
	response, err := getter.RequestHandler(url, "?"+data, "GET", *certFile, *keyFile)
	if err != nil {
		logger.Printf("Error retrieving publication with %s", url+"?"+data)
		logger.Fatal(err)
	}
	logger.Print("Got publications, I'm sending them to competent people...")

	responseBYTE := []byte(response)
	// TODO: avoid interface, define schema!
	var responseJSON []getter.RestOutput
	json.Unmarshal(responseBYTE, &responseJSON)

	// buffer users
	ch := make(chan []map[string]interface{}, 100)

	// buffer for splitting
	go getter.SplitFiles(responseJSON, ch, logger)

	// got tasks/files per user, then send
	url = "127.0.0.1:3126"
	for i := 0; i < 10; i++ {
		go getter.SendTask(ch, url, logger)
	}
	return
}
