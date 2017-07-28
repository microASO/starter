package main

import (
	"encoding/json"
	"flag"
	"log"
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
	// TODO: convert response json
	responseJSON, err := json.Marshal([]byte(response))
	if err != nil {
		log.Fatalf("Error converting respose into interface: " + err.Error())
	}

	// buffer users
	ch := make(chan []byte, 100)

	// get tasks/files per user
	for i := 0; i <= 10; i++ {
		go getter.GetFiles(ch, []byte(strconv.Itoa(i)))
	}

	// send info (TODO: send directly json file)
	url = "http://localhost:3126/task"
	data = "data=" + string(responseJSON)

	for i := 0; i <= 10; i++ {
		cc, _ := getter.ProvidePublication(<-ch)
		log.Println("char: ", string(cc))
	}

}
