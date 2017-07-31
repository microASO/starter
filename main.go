package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/microASO/starter/getter"
)

type configuration struct {
	Proxy   string `json:"proxy"`
	LogPath string `json:"logPath"`
}

func main() {
	// get configuration
	var err error
	absPath, _ := filepath.Abs("config/conf.json")
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

	// check for command line parameter, but keep the conf.json as the default
	var (
		certFile = flag.String("cert", configuration.Proxy, "A PEM eoncoded certificate file.")
		keyFile  = flag.String("key", configuration.Proxy, "A PEM encoded private key file.")
		logFile  = flag.String("out", configuration.LogPath, "Redirect output to this file. Default stdout")
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

	// define query endpoint
	url := "https://cmsweb-testbed.cern.ch/crabserver/preprod/filetransfers"
	// TODO: url encode parameters later
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
	logger.Print("Got publications, processing tasks...")

	responseBYTE := []byte(response)
	var responseJSON getter.RestOutput
	json.Unmarshal(responseBYTE, &responseJSON)

	// buffer users
	ch := make(chan []getter.ResultSchema, 100)

	// buffer for splitting
	go getter.SplitFiles(responseJSON, ch, logger)

	// got tasks/files per user, then send
	url = "127.0.0.1:3126"
	for i := 0; i < 10; i++ {
		go getter.SendTask(ch, url, logger)
	}
	time.Sleep(time.Second * 5)
	return
}
