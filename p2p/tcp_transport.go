package p2p

import (
	"fmt"
	"net"
	"sync"
)

// TCPPeer represents the remote node over a TCP extablished connection
type TCPPeer struct {
	// conn is underlying connection of the peer
	conn net.Conn

	// If we dial and retrieve a conn => outbound == true
	// If we accept and retrieve a conn => outbound == false
	outbound bool
}

// constructor function for peer
func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		conn:     conn,
		outbound: outbound,
	}
}

type TCPTransport struct {
	listenAddress string
	listener      net.Listener

	mu    sync.RWMutex // In golang this mutex protects the below peer
	peers map[net.Addr]Peer
}

// constructor function
// Go automatically initializes all struct fields to their zero values

/*
❌ The Manual (Broken) Way:
// Inside main.go
// This will throw a compiler error because listenAddress is private!
// And even if it worked, the peers map would be nil and crash later.
transport := &p2p.TCPTransport{
    listenAddress: ":3000",
}

✅ The Constructor (Safe) Way:
go
// Inside main.go
// Simple, secure, and internal maps are perfectly initialized.
transport := p2p.NewTCPTransport(":3000")
*/

func NewTCPTransport(listenAddr string) *TCPTransport {
	return &TCPTransport{
		listenAddress: listenAddr,
		peers:         make(map[net.Addr]Peer),
	}
}

// ListenAndAccept initializes the network listener and starts looking for other nodes
func (t *TCPTransport) ListenAndAccept() error {
	var err error

	// 1. Bind to the network socket using the address stored in our struct
	t.listener, err = net.Listen("tcp", t.listenAddress)
	if err != nil {
		return err
	}

	// 2. Start the infinite loop to accept incoming connections in the background
	go t.startAcceptLoop()

	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	fmt.Printf("Listening for connection requests at %s\n", t.listenAddress)
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
			continue
		}
		go t.handleConnection(conn)
	}
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	fmt.Printf("New incoming connection from: %s\n", conn.RemoteAddr())
}
