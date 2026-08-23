package main

import (
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	address := os.Getenv("AGENTKIT_DEMO_ADDR")
	if address == "" {
		address = "127.0.0.1:8485"
	}
	server := &http.Server{
		Addr: address, Handler: newAPIServer().handler(),
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 75 * time.Second,
	}
	log.Printf("stateful AgentKit demo listening on http://%s", address)
	log.Fatal(server.ListenAndServe())
}
