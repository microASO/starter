package main

import (
	"fmt"

	"github.com/microASO/starter/getter"
)

func main() {
	fmt.Print("ciao\n")
	_ = getter.GetFiles()
}
