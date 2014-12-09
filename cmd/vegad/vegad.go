package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/vektra/vega"
	"github.com/vektra/vega/cluster"
)

var fPort = flag.Int("port", vega.DefaultPort, "port to listen on localhost")
var fClusterPort = flag.Int("cluster-port", cluster.DefaultClusterPort, "port to listen on for cluster membership")
var fHttpPort = flag.Int("http-port", vega.DefaultHTTPPort, "port to listen on")
var fData = flag.String("data-dir", cluster.DefaultPath, "path to store data in")
var fAdvertise = flag.String("advertise", "", "Address to advertise vega on")

func main() {
	flag.Parse()

	cfg := &cluster.ConsulNodeConfig{
		ListenPort:    *fClusterPort,
		DataPath:      *fData,
		AdvertiseAddr: *fAdvertise,
	}

	node, err := cluster.NewConsulClusterNode(cfg)
	if err != nil {
		log.Fatalf("unable to create node: %s", err)
		os.Exit(1)
	}

	go node.Accept()

	var h *vega.HTTPService
	var local *vega.Service

	if *fHttpPort != 0 {
		h = vega.NewHTTPService(
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

	if *fPort != 0 {
		local, err = vega.NewService(fmt.Sprintf("127.0.0.1:%d", *fPort), node)
		if err != nil {
			log.Fatalf("Unable to create local server: %s", err)
			os.Exit(1)
		}

		go local.AcceptInsecure()
	}

	fmt.Printf("! Booted vegad:\n")
	fmt.Printf("* LocalPort: %d\n", *fPort)
	fmt.Printf("* ClusterPort: %d\n", cfg.ListenPort)
	fmt.Printf("* DataPath: %s\n", cfg.DataPath)
	fmt.Printf("* AdvertiseId: %s\n", cfg.AdvertiseID())

	if h == nil {
		fmt.Printf("* HTTP Server disabled\n")
	} else {
		fmt.Printf("* HTTP Server: %d\n", *fHttpPort)
	}

	sig := make(chan os.Signal)

	signal.Notify(sig, os.Interrupt)

	<-sig

	fmt.Printf("\nGracefully shutting down...\n")

	if h != nil {
		h.Close()
	}

	if local != nil {
		local.Close()
	}

	node.Cleanup()
	node.Close()
}
