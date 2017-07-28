package main

import (
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
	response, err := getter.RequestHandler(url, "?"+data, "GET", *certFile, *keyFile)
	if err != nil {
		log.Fatalf("Error retrieving publication with %s", url+"?"+data+"\n"+err.Error())
	}

	log.Print(response)
	// TODO: convert response json

	ch := make(chan []byte, 100)

	for i := 0; i <= 10; i++ {
		go getter.GetFiles(ch, []byte(strconv.Itoa(i)))
	}

	for i := 0; i <= 10; i++ {
		cc, _ := getter.ProvidePublication(<-ch)
		log.Println("char: ", string(cc))
	}

}
