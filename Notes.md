# Distributed File Storage — Networking Layer Notes

> **Status:** TCP transport ✅ · Protocol/serialization ✅ · Handshake integration into `TCPPeer` 🔄 · Message framing & loop ⏳

## Table of Contents

1. [Big Picture](#1-big-picture)
2. [Core Concepts](#2-core-concepts)
3. [The Handshake](#3-the-handshake)
4. [Wire Format Design](#4-wire-format-design)
5. [Serialization with `encoding/binary`](#5-serialization-with-encodingbinary)
6. [Protocol Code (`protocol.go`)](#6-protocol-code-protocolgo)
7. [Integrating the Handshake into `TCPPeer`](#7-integrating-the-handshake-into-tcppeer)
8. [Testing Two Nodes](#8-testing-two-nodes)
9. [Message Framing & the Message Loop](#9-message-framing--the-message-loop)
10. [Progress & Next Steps](#10-progress--next-steps)

---

## 1. Big Picture

With the basic transport layer in place, the networking foundation needs two more pieces:

1. **Handshake**: establish who is on the other side and which protocol we are speaking.
2. **Message loop ("conveyor")**: continuously receive, decode and dispatch messages between peers.

Target architecture:

```
                 NODE
                  │
       ┌──────────┴──────────┐
       │                     │
   File system           Networking
                             │
                       TCPTransport
                             │
                          TCPPeer
                             │
                    ┌────────┴────────┐
                    │                 │
                Handshake         Message loop
                                      │
                                  Encode/Decode
                                      │
                                  Dispatch
                                      │
                              Storage operations
```

End-to-end data path:

```
TCP connection → Handshake → Message framing → Encode/Decode
→ Receive loop → Dispatch → PUT / GET / DELETE → Replication
```

---

## 2. Core Concepts

### `net.Conn` is a byte pipe

Once the listener accepts a connection:

```go
conn, err := t.listener.Accept()
// conn is a net.Conn
```

`net.Conn` is simply a pipe through which bytes flow in both directions:

```
Node A                         Node B

TCPPeer                        TCPPeer
   │                              │
   └─────── net.Conn ─────────────┘
              bytes <──────>
```

TCP knows nothing about `"Hello, I am node A"`, `"Give me file X"` or `"Store this chunk"`. It only knows **bytes**. Our application must define a **protocol** on top of it.

### TCP is a stream, not a message system

If A sends `HELLO` followed by `STORE file.txt`, B may receive either of:

```
HELLOSTORE file.txt          (coalesced)
HELLO / STORE file.txt       (separate)
```

TCP does **not** preserve application-level message boundaries. We therefore need a **message framing** protocol.

### Mental model

| Concept | Role |
|---|---|
| `net.Conn` | A byte pipe between two nodes |
| `TCPPeer` | One remote node I talk to, via a `net.Conn` |
| `TCPTransport` | Accepts/dials connections and manages many peers |
| Protocol | The rules for what the bytes mean (handshake, messages…) |

```
TCPTransport                 TCPPeer
   ├── Peer A                   │
   ├── Peer B                TCPPeer ──► net.Conn
   └── Peer C
```

> This separation matters a lot once replication is added.

### Peer interface

A `Peer` represents the communication endpoint of another node. Conceptually:

```
Peer
 ├── RemoteAddress
 ├── Send(message)
 ├── Receive()
 └── Close()
```

Not everything is needed immediately, but the design should move in this direction. Responsibilities:

- `TCPPeer` → communication with **one** remote node.
- `TCPTransport` → managing **many** peers.

Inbound and outbound connections both end up as the same `TCPPeer` abstraction, which is why `TCPPeer` has an `outbound bool` field.

---

## 3. The Handshake

When Node A connects to Node B, B only knows *"I have a TCP connection."* It does not know:

- Who is this node?
- Is it one of my storage nodes?
- What protocol version does it speak?
- What should I expect next?

The handshake means: *"Before exchanging real storage messages, let's agree on who we are and how we'll communicate."*

```
Node A                                  Node B
  │──── TCP connection ───────────────────>│
  │──── HANDSHAKE ────────────────────────>│
  │     NodeID = "node-A", Version = 1      │
  │<─── HANDSHAKE RESPONSE ────────────────│
  │     NodeID = "node-B", Version = 1      │
  │         Connection established          │
```

**Keep v1 minimal.** The handshake carries only:

1. `NodeID`
2. `ProtocolVersion`

No authentication, encryption, capabilities or compression negotiation yet.

### Who sends first?

`TCPPeer.outbound` decides:

```
if outbound:  we created the connection → send handshake first, then receive
else:         remote created it         → receive handshake first, then send
```

```
Outbound:  A ─ connection ─> B      Inbound:  A ─ connection ─> B
           A ─ handshake ──> B                A <─ handshake ── B
```

Both sides must follow a consistent order, otherwise both wait for the other and **deadlock**.

---

## 4. Wire Format Design

`conn.Write()` accepts `[]byte`, not a Go struct, so we need to solve two **separate** problems:

| Problem | Question |
|---|---|
| **Serialization** | How do I turn the handshake into bytes? |
| **Framing** | How does the receiver know how many bytes belong to it? |

A single `conn.Write` of 20 bytes may arrive as `Read #1 → 8 bytes`, `Read #2 → 12 bytes`.

### Handshake layout

```
┌──────────────────────────────┐
│ Header                       │
│   Version   uint32  (4 bytes)│
│   Length    uint16  (2 bytes)│
├──────────────────────────────┤
│ Payload                      │
│   NodeID    Length bytes     │
└──────────────────────────────┘
```

- `Length` = number of bytes in the payload that follows the header.
- Receiver logic: read the fixed 6-byte header → learn `Length` → read exactly `Length` more bytes.

Example (`Version = 1`, `NodeID = "node-A"`):

```
00 00 00 01 | 00 06 | 6E 6F 64 65 2D 41
  Version      Len      "node-A"
```

> **Open design question:** is `uint16` (max 65 535 bytes) enough for `Length`? Fine for node IDs; revisit (likely `uint32`) for general messages.

---

## 5. Serialization with `encoding/binary`

`encoding/binary` doesn't encode arbitrary Go structs; it converts **fixed-size values** to bytes in a defined byte order. It forces us to understand what actually travels over the wire.

- **Byte order:** use **Big Endian** (network byte order). `Version = 1` → `00 00 00 01`.
- `binary.Write(w io.Writer, order, data)`: struct → bytes. Fields are written in **struct order**.
- `binary.Read(r io.Reader, order, &data)`: bytes → struct. Needs a **pointer**.

### In-memory round trip

```go
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Header struct {
	Version uint32
	Length  uint16
}

func main() {
	header := Header{Version: 1, Length: 6}

	buf := new(bytes.Buffer)

	// Struct -> bytes
	if err := binary.Write(buf, binary.BigEndian, header); err != nil {
		panic(err)
	}
	fmt.Printf("Encoded bytes: %v\n", buf.Bytes()) // [0 0 0 1 0 6]
	fmt.Printf("Header size: %d bytes\n", buf.Len()) // 6

	// Bytes -> struct (consumes bytes from the buffer, like a queue)
	var decoded Header
	if err := binary.Read(buf, binary.BigEndian, &decoded); err != nil {
		panic(err)
	}
	fmt.Printf("Deserialized: %+v\n", decoded) // {Version:1 Length:6}
}
```

### Why `io.Reader` / `io.Writer` matter

`binary.Write` only needs an `io.Writer`; `binary.Read` only needs an `io.Reader`. A `bytes.Buffer`, a file and a `net.Conn` all qualify:

```go
binary.Write(buf,  binary.BigEndian, header) // in-memory
binary.Write(file, binary.BigEndian, header) // file
binary.Write(conn, binary.BigEndian, header) // TCP connection
```

So no intermediate `bytes.Buffer` is required between nodes:

```
            TCPPeer
               │
             conn
        ┌──────┴──────┐
    io.Writer     io.Reader
        │             │
  binary.Write   binary.Read
```

> ⚠️ `binary.Read` blocks until the requested bytes arrive or an error occurs. Handle connection failures and **timeouts** (e.g. `conn.SetReadDeadline`) during the handshake.

---

## 6. Protocol Code (`protocol.go`)

Suggested layout, keeping the protocol separate from the transport so other transports can reuse it:

```
p2p/
├── transport.go
├── tcp_transport.go
└── protocol.go
```

```go
package p2p

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const ProtocolVersion uint32 = 1

// Header is the fixed-size metadata at the start of a message.
//
//	┌──────────────┬──────────────┐
//	│ Version      │ Length       │
//	│ 4 bytes      │ 2 bytes      │
//	└──────────────┴──────────────┘
//
// Length is the number of payload bytes following the header.
type Header struct {
	Version uint32
	Length  uint16
}

// Handshake identifies a node when two peers connect.
// For now it only carries the NodeID.
type Handshake struct {
	NodeID string
}

// Encode converts the Header into bytes.
func (h *Header) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, h); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeHeader reads a Header from an io.Reader.
func DecodeHeader(r io.Reader) (*Header, error) {
	var header Header
	if err := binary.Read(r, binary.BigEndian, &header); err != nil {
		return nil, err
	}
	return &header, nil
}

// Encode converts a Handshake into its wire format: Header followed by NodeID.
func (h *Handshake) Encode() ([]byte, error) {
	payload := []byte(h.NodeID)

	// Header.Length is a uint16, so the NodeID must fit.
	if len(payload) > 65535 {
		return nil, fmt.Errorf("node ID is too large")
	}

	header := Header{
		Version: ProtocolVersion,
		Length:  uint16(len(payload)),
	}

	headerBytes, err := header.Encode()
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	buf.Write(headerBytes)
	buf.Write(payload)
	return buf.Bytes(), nil
}

// DecodeHandshake reads a Handshake from an io.Reader:
// read header -> check version -> read exactly Length bytes -> NodeID.
func DecodeHandshake(r io.Reader) (*Handshake, error) {
	header, err := DecodeHeader(r)
	if err != nil {
		return nil, err
	}

	if header.Version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", header.Version)
	}

	payload := make([]byte, header.Length)

	// io.ReadFull is essential: a plain Read() may return fewer bytes than
	// requested because TCP is a stream. ReadFull keeps reading until the
	// whole payload arrives or an error occurs.
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return &Handshake{NodeID: string(payload)}, nil
}
```

### Why `io.ReadFull`

```
r.Read(payload)      ✗  may return 3 bytes, then 3 bytes
io.ReadFull(r, buf)  ✓  loops over Read() until exactly len(buf) bytes
```

### Test the protocol in memory first (no TCP)

```go
func main() {
	original := p2p.Handshake{NodeID: "node-A"}

	data, err := original.Encode()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Wire data: %v\n", data)

	decoded, err := p2p.DecodeHandshake(bytes.NewReader(data))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Decoded handshake: %+v\n", *decoded)
}
```

Expected:

```
Wire data: [0 0 0 1 0 6 110 111 100 101 45 65]
Decoded handshake: {NodeID:node-A}
```

---

## 7. Integrating the Handshake into `TCPPeer`

### Who knows what

A peer needs to distinguish **local** and **remote** identity:

```
t.id     → MY identity      (owned by TCPTransport)
peer.id  → REMOTE identity  (discovered during handshake)
```

```
TCPTransport
   │ owns local NodeID
   │ creates
   ▼
TCPPeer  ── id = "" until handshake ──► id = remote NodeID
```

### Structs and constructors

```go
type TCPTransport struct {
	listenAddress string
	listener      net.Listener

	// id identifies THIS node.
	id string

	mu    sync.RWMutex
	peers map[net.Addr]Peer
}

func NewTCPTransport(listenAddr string, id string) *TCPTransport {
	return &TCPTransport{
		listenAddress: listenAddr,
		id:            id,
		// Struct zero-values do NOT initialize maps; writing to a nil map panics.
		peers: make(map[net.Addr]Peer),
	}
}

type TCPPeer struct {
	// conn is the underlying TCP connection.
	conn net.Conn

	// outbound: true if we dialed (net.Dial), false if we accepted (Accept).
	outbound bool

	// id is the REMOTE node's identity, learned during the handshake.
	id string
}

// The caller must state who initiated the connection.
// The local node ID is deliberately NOT part of the peer.
func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{conn: conn, outbound: outbound}
}

func (p *TCPPeer) ID() string { return p.id }
```

Update the `Peer` interface so peers can be looked up by identity later:

```go
type Peer interface {
	ID() string
}
```

> Because the constructor signature changed, call sites must pass the ID:
> `p2p.NewTCPTransport(":3000", "node-A")`.

### Handshake methods

```go
func (p *TCPPeer) Handshake(localNodeID string) error {
	if p.outbound {
		// We initiated: send first, then receive.
		if err := p.sendHandshake(localNodeID); err != nil {
			return err
		}
		return p.receiveHandshake()
	}

	// Remote initiated: receive first, then send.
	if err := p.receiveHandshake(); err != nil {
		return err
	}
	return p.sendHandshake(localNodeID)
}

func (p *TCPPeer) sendHandshake(localNodeID string) error {
	data, err := (&Handshake{NodeID: localNodeID}).Encode()
	if err != nil {
		return err
	}
	_, err = p.conn.Write(data)
	return err
}

func (p *TCPPeer) receiveHandshake() error {
	hs, err := DecodeHandshake(p.conn)
	if err != nil {
		return err
	}
	p.id = hs.NodeID // remote identity
	return nil
}
```

```
OUTBOUND                           INBOUND
sendHandshake()    ───────────►    receiveHandshake()
receiveHandshake() ◄───────────    sendHandshake()
```

### Using it in `handleConnection`

```go
func (t *TCPTransport) handleConnection(conn net.Conn) {
	fmt.Printf("New incoming connection from: %s\n", conn.RemoteAddr())

	// We accepted this connection, so it is inbound.
	peer := NewTCPPeer(conn, false)

	if err := peer.Handshake(t.id); err != nil {
		fmt.Printf("Handshake failed: %s\n", err)
		conn.Close()
		return
	}

	fmt.Printf("Handshake completed with peer: %s\n", peer.ID())

	// TODO: register peer, start message loop
}
```

Target flow:

```
handleConnection → TCPPeer → Handshake → Register peer → Start message loop
```

### Layer responsibilities

| Layer | Question it answers |
|---|---|
| Protocol | How do I encode/decode a handshake? |
| Peer | How do I perform the handshake over this connection? |
| Transport | How do I accept/dial and manage multiple peers? |

---

## 8. Testing Two Nodes

The server **must** also run the handshake in `handleConnection`; otherwise the dialing node waits forever for a reply.

Temporary `main.go` harness (module name assumed `Distributed_file_storage`):

```go
package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"Distributed_file_storage/p2p"
)

func main() {
	// NODE A: listens on :3000
	transport := p2p.NewTCPTransport(":3000", "node-A")
	if err := transport.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Node A is running on :3000")

	time.Sleep(time.Second) // let the listener start

	// NODE B: manually dial Node A (outbound = true)
	conn, err := net.Dial("tcp", "localhost:3000")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Node B connected to Node A")

	peer := p2p.NewTCPPeer(conn, true)
	if err := peer.Handshake("node-B"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Handshake completed!")
	fmt.Println("Remote peer ID:", peer.ID())

	select {} // keep the process alive
}
```

Expected output:

```
Node A is running on :3000
Node B connected to Node A
New incoming connection from: 127.0.0.1:xxxxx
Handshake completed with peer: node-B
Handshake completed!
Remote peer ID: node-A
```

Each side learned the other's identity over a real TCP connection:

```
Node A: peer.ID() == "node-B"        Node B: peer.ID() == "node-A"
```

> This is only a test harness. Next, move `net.Dial()` into `TCPTransport` so it handles both accepting **and** dialing, and register both inbound and outbound peers in `TCPTransport.peers`.

---

## 9. Message Framing & the Message Loop

### Message frame

After the handshake, every message needs framing:

```
┌──────────────┬───────────────┬──────────────┐
│ length       │ message type  │ payload      │
└──────────────┴───────────────┴──────────────┘
```

The receiver can then conclude: *"The next N bytes belong to one message."*

### Message conveyor (receive path)

```
TCP connection → Receive bytes → Decode message → Identify message → Handle message
```

### Send path

```
Application → Create message → Encode → Write to net.Conn → TCP
```

---

## 10. Progress & Next Steps

- [x] **TCP transport**: `ListenAndAccept()` → `handleConnection(conn)`
- [x] **Protocol/serialization**: `Header`, `Handshake`, `Encode()`, `DecodeHandshake()`, `io.ReadFull`
- [ ] **Integrate handshake into `TCPPeer`/`TCPTransport`** (in progress)
- [ ] Move dialing into `TCPTransport`; register inbound + outbound peers in `peers`
- [ ] Add read/write deadlines for the handshake
- [ ] Decide on `Length` width (`uint16` vs `uint32`) and a max message size guard
- [ ] Message framing: `length | type | payload`
- [ ] Message receive loop + dispatch
- [ ] Storage operations: `PUT` / `GET` / `DELETE`
- [ ] Replication