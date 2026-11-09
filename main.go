package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/anyone21/dummy-file-store/p2p"
)

// func main() {
// 	// 1. Initialize the TCP Transport via the constructor engine
// 	// We will listen on local port 3000
// 	tr := p2p.NewTCPTransport(":3000")

// 	// 2. Start running the network listener loop in the background
// 	fmt.Println("Starting the distributed storage node transport layer...")
// 	if err := tr.ListenAndAccept(); err != nil {
// 		log.Fatalf("Critical error starting transport: %v", err)
// 	}

// 	// 3. Keep the main execution thread alive indefinitely.
// 	// Because ListenAndAccept runs inside a background goroutine,
// 	// this empty select statement blocks main from exiting and dropping the process.
// 	select {}
// }

func main() {

	original := p2p.Handshake{
		NodeID: "node-A",
	}

	// Encode the handshake.
	data, err := original.Encode()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Wire data: %v\n", data)

	// Simulate receiving the bytes.
	reader := bytes.NewReader(data)

	// Decode the handshake.
	decoded, err := p2p.DecodeHandshake(reader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decoded handshake: %+v\n", *decoded)
}
