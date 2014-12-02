package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vektra/vega"
)

var fListen = flag.String("listen", "", "Name to listen on")
var fConnect = flag.String("connect", "", "Name to connect to")

func main() {
	flag.Parse()

	if *fListen != "" {
		listen()
	} else if *fConnect != "" {
		connect()
	} else {
		fmt.Fprint(os.Stderr, "Provide listen or connect\n")
		os.Exit(1)
	}
}

func listen() {
	addr := *fListen

	fc, err := vega.Local()
	if err != nil {
		panic(err)
	}

	pc, err := fc.ListenPipe(addr)
	if err != nil {
		panic(err)
	}

	go io.Copy(pc, os.Stdin)
	io.Copy(os.Stdout, pc)
}

func connect() {
	addr := *fConnect

	fc, err := vega.Local()
	if err != nil {
		panic(err)
	}

	pc, err := fc.ConnectPipe(addr)
	if err != nil {
		panic(err)
	}

	go io.Copy(pc, os.Stdin)
	io.Copy(os.Stdout, pc)
}
