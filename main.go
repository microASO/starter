package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

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
	data := "subresource=acquiredTransfers&asoworker=asodciangot1&grouping=0"

	// make request
	// get users to publish
	response, err := getter.RequestHandler(url, "?"+data, "GET", *certFile, *keyFile, logger)
	if err != nil {
		logger.Printf("Error retrieving publication with %s", url+"?"+data)
		logger.Fatal(err)
	}

	logger.Print(response)

	responseBYTE := []byte(response)
	// TODO: avoid interface, define schema!
	var responseJSON []interface{}
	json.Unmarshal(responseBYTE, &responseJSON)

	// buffer users
	ch := make(chan []byte, 100)

	// get tasks/files per user and then send
	url = "127.0.0.1:3126"
	for i := range responseJSON {
		go getter.GetFiles(ch, []byte(strconv.Itoa(i)), logger)
		go getter.SendTask(ch, url, logger)
	}
	return
}
