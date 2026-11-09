package main

import (
	"fmt"
	"log"
	"net"
	"time"

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

// func main() {

// 	original := p2p.Handshake{
// 		NodeID: "node-A",
// 	}

// 	// Encode the handshake.
// 	data, err := original.Encode()
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	fmt.Printf("Wire data: %v\n", data)

// 	// Simulate receiving the bytes.
// 	reader := bytes.NewReader(data)

// 	// Decode the handshake.
// 	decoded, err := p2p.DecodeHandshake(reader)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	fmt.Printf("Decoded handshake: %+v\n", *decoded)
// }

// Testing handshake connection
func main() {
	// --------------------------------------------------
	// NODE A
	// --------------------------------------------------
	//
	// This node listens for incoming TCP connections.
	//
	// Node ID = node-A
	// Address = :3000
	transport := p2p.NewTCPTransport(":3000", "node-A")

	// Start listening.
	err := transport.ListenAndAccept()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Node A is running on :3000")

	// --------------------------------------------------
	// Give the listener a moment to start.
	// --------------------------------------------------
	time.Sleep(time.Second)

	// --------------------------------------------------
	// NODE B
	// --------------------------------------------------
	//
	// For this simple test, instead of creating a second
	// TCPTransport, we manually dial Node A.
	//
	// This represents another node connecting to us.
	conn, err := net.Dial("tcp", "localhost:3000")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	fmt.Println("Node B connected to Node A")
	// Create a TCPPeer representing Node A from
	// Node B's perspective.
	//
	// outbound = true because Node B called net.Dial().
	peer := p2p.NewTCPPeer(conn, true)

	// Perform the handshake.
	err = peer.Handshake("node-B")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Handshake completed!")
	fmt.Println("Remote peer ID:", peer.ID())

	// Keep the program alive so the listener doesn't exit.
	select {}
}
