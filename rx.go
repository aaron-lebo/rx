package main

import (
	"github.com/aaron-lebo/rx/collect"
	"github.com/aaron-lebo/rx/filter"
	. "github.com/aaron-lebo/rx/lib"
	"os"
)

func main() {
	command := os.Args[1]
	os.MkdirAll("out/"+command, 0755)
	LoadHistory(command)
	if command == "collect" {
		collect.Run()
	} else {
		filter.Run()
	}
}
