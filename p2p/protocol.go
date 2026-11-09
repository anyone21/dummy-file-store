package p2p

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// ProtocolVersion identifies the version of the communication
// protocol used by our distributed file-storage nodes.
//
// Both nodes should use the same protocol version when
// communicating with each other.
const ProtocolVersion uint32 = 1

// Header is the fixed-size metadata placed at the beginning
// of every message.
//
// The header tells the receiver how to interpret the bytes
// that follow it.
//
// Wire format:
//
//	┌──────────────┬──────────────┐
//	│ Version      │ Length       │
//	│ 4 bytes      │ 2 bytes      │
//	└──────────────┴──────────────┘
//
// Version = protocol version
// Length  = number of bytes in the payload
type Header struct {
	// Version identifies which version of our wire protocol
	// is being used.
	//
	// We use uint32, which occupies 4 bytes.
	Version uint32

	// Length tells the receiver how many bytes it should
	// read for the payload following this header.
	//
	// We use uint16, which occupies 2 bytes and can represent
	// payloads up to 65,535 bytes.
	Length uint16
}

// Handshake identifies a node when two peers establish
// a connection.
//
// The handshake payload currently contains only the NodeID.
// We can add more fields later if our protocol requires them.

type Handshake struct {
	NodeID string
}

// Encode converts the Header into its binary representation.
//
// The result is a []byte that can eventually be written to
// a TCP connection.
//
// We use bytes.Buffer because it implements io.Writer,
// which is what binary.Write requires.
//
//	Header
//	   ↓
//	binary.Write()
//	   ↓
//	bytes.Buffer
//	   ↓
//	[]byte
func (h *Header) Encode() ([]byte, error) {
	// Create an in-memory buffer to hold the encoded bytes.
	buf := new(bytes.Buffer)

	// Serialize the Header into the buffer.
	//
	// binary.BigEndian specifies the byte order used by
	// our protocol. Both sender and receiver must use the
	// same byte order.
	err := binary.Write(buf, binary.BigEndian, h)
	if err != nil {
		return nil, err
	}

	// Return the encoded bytes.
	return buf.Bytes(), nil
}

// DecodeHeader reads a binary Header from an io.Reader.
//
// The reader could be:
//
//   - a TCP connection
//   - a bytes.Buffer
//   - a file
//   - anything else implementing io.Reader
//
// This is important because our protocol code doesn't need
// to know that TCP is being used.
func DecodeHeader(r io.Reader) (*Header, error) {
	// Create an empty Header that will be populated
	// by binary.Read().
	var header Header

	// Read bytes from the reader and deserialize them
	// into the Header structure.
	//
	// binary.Read() expects an io.Reader, and net.Conn
	// implements io.Reader, so eventually we can do:
	//
	//	header, err := DecodeHeader(conn)
	err := binary.Read(r, binary.BigEndian, &header)
	if err != nil {
		return nil, err
	}

	// Return the decoded Header.
	return &header, nil
}

// EncodeHandshake converts a Handshake into bytes.
//
// The final wire format is:
//
//	┌──────────────────────┬─────────────────────┐
//	│ Header               │ Payload             │
//	│                      │                     │
//	│ Version: 4 bytes     │ NodeID: N bytes     │
//	│ Length: 2 bytes      │                     │
//	└──────────────────────┴─────────────────────┘
func (h *Handshake) Encode() ([]byte, error) {
	// Convert the NodeID string into bytes.
	payload := []byte(h.NodeID)

	// Our Header>Length is uint16, so make sure that
	// NodeID fits inside that field.
	if len(payload) > 65535 {
		return nil, fmt.Errorf("node ID is too large")
	}

	// create the header describing this handshake
	header := Header{
		Version: ProtocolVersion,
		Length:  uint16(len(payload)),
	}

	// Encode the header first
	headerBytes, err := header.Encode()
	if err != nil {
		return nil, err
	}

	// Create a buffer large enough to contain both
	// the header and the payload.
	buf := new(bytes.Buffer)

	// Write the header
	buf.Write(headerBytes)

	// write the NodeID immediately after the header
	buf.Write(payload)

	return buf.Bytes(), nil
}

// DecodeHandshake reads a handshake from an io.Reader.
//
// It first reads the fixed-size Header. The Header tells us
// how many bytes belong to the handshake payload.
//
// Then we read exactly that many bytes and convert them
// back into the NodeID string.

func DecodeHandshake(r io.Reader) (*Handshake, error) {
	// First read the fixed-size header.
	header, err := DecodeHeader(r)
	if err != nil {
		return nil, err
	}

	// Make sure the remote node is speaking a protocol
	// version that we understand.
	if header.Version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", header.Version)
	}

	// Allocate a Byte slice larger enought to hold the payload
	// described by header
	payload := make([]byte, header.Length)
	// Read exactly header.Length bytes from the reader.
	//
	// io.ReadFull is important here.
	//
	// A normal Read() is allowed to return fewer bytes
	// than requested because TCP is a stream.
	//
	// ReadFull keeps reading until the entire payload
	// has been received or an error occurs.
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return nil, err
	}

	// Convert the payload bytes back into the NodeID string.
	return &Handshake{
		NodeID: string(payload),
	}, nil
}
