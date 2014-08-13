package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	mailbox "./.."
)

var fPort = flag.Int("port", mailbox.DefaultPort, "port to listen on")
var fHttpPort = flag.Int("http-port", mailbox.DefaultHTTPPort, "port to listen on")
var fData = flag.String("data-dir", mailbox.DefaultPath, "path to store data in")

func main() {
	flag.Parse()

	cfg := &mailbox.ConsulNodeConfig{
		ListenPort: *fPort,
		DataPath:   *fData,
	}

	node, err := mailbox.NewConsulClusterNode(cfg)
	if err != nil {
		log.Fatalf("unable to create node: %s", err)
		os.Exit(1)
	}

	go node.Accept()

	var h *mailbox.HTTPService

	if *fHttpPort != 0 {
		h = mailbox.NewHTTPService(
			fmt.Sprintf("127.0.0.1:%d", *fHttpPort),
			node.Registry())

		err = h.Listen()
		if err != nil {
			log.Fatalf("unable to create http server: %s", err)
			os.Exit(1)
		}

		h.BackgroundTimeouts()
		go h.Accept()
	}

	fmt.Printf("Booted mbd:\n")
	fmt.Printf("  ListenPort: %d\n", cfg.ListenPort)
	fmt.Printf("  DataPath: %s\n", cfg.DataPath)
	fmt.Printf("  AdvertiseId: %s\n", cfg.AdvertiseID())

	if h == nil {
		fmt.Printf("  HTTP Server disabled\n")
	} else {
		fmt.Printf("  HTTP Server: %d\n", *fHttpPort)
	}

	sig := make(chan os.Signal)

	signal.Notify(sig, os.Interrupt)

	<-sig

	fmt.Printf("\nGracefully shutting down...\n")

	if h != nil {
		h.Close()
	}

	node.Cleanup()
	node.Close()
}
