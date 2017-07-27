package main

import (
	"log"
	"strconv"

	"github.com/microASO/starter/getter"
)

func main() {
	log.Print("Processing Started\n")
	ch := make(chan []byte, 100)

	for i := 0; i <= 10; i++ {
		go getter.GetFiles(ch, []byte(strconv.Itoa(i)))
	}

	for i := 0; i <= 10; i++ {
		cc, _ := getter.ProvidePublication(<-ch)
		log.Println("char: ", string(cc))
	}

}
