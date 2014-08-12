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
	}

	go node.Accept()

	fmt.Printf("Booted mbd:\n")
	fmt.Printf("  ListenPort: %d\n", cfg.ListenPort)
	fmt.Printf("  DataPath: %s\n", cfg.DataPath)
	fmt.Printf("  AdvertiseId: %s\n", cfg.AdvertiseID())

	sig := make(chan os.Signal)

	signal.Notify(sig, os.Interrupt)

	<-sig

	fmt.Printf("\nGracefully shutting down...\n")

	node.Cleanup()
	node.Close()
}
