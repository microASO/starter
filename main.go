package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/microASO/starter/getter"
)

var (
	certFile = flag.String("cert", "usercert.pem", "A PEM eoncoded certificate file.")
	keyFile  = flag.String("key", "unencrypted.pem", "A PEM encoded private key file.")
)

func main() {
	log.Print("Processing Started\n")
	flag.Parse()
	log.Printf("Using certfile: %s and keyfile: %s", *certFile, *keyFile)

	// define query endpoint
	url := "https://cmsweb-testbed.cern.ch/crabserver/preprod/filetransfers"
	data := "subresource=acquiredTransfers&asoworker=asodciangot1&grouping=0"

	// make request
	// get users to publish
	response, err := getter.RequestHandler(url, "?"+data, "GET", *certFile, *keyFile)
	if err != nil {
		log.Fatalf("Error retrieving publication with %s", url+"?"+data+"\n"+err.Error())
	}

	log.Print(response)

	responseBYTE := []byte(response)
	// TODO: avoid interface, define schema!
	var responseJSON []interface{}
	json.Unmarshal(responseBYTE, &responseJSON)

	// buffer users
	ch := make(chan []byte, 100)

	// get tasks/files per user
	for i := range responseJSON {
		go getter.GetFiles(ch, []byte(strconv.Itoa(i)))
	}

	// send info (TODO: send directly json file)
	url = "http://localhost:3126/task"
	data = "data=" + response
	_, err = http.Post(url, "application/json", bytes.NewBuffer(responseBYTE))
	if err != nil {
		log.Printf("Error sending %s \n", response)
		log.Fatal(err)
	}

	for i := 0; i <= 10; i++ {
		cc, _ := getter.ProvidePublication(<-ch)
		log.Println("char: ", string(cc))
	}

}
