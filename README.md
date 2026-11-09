This Project is inspired from @anthonygg_ - "Distributed File storage in GO"

## Network Layer
1. Setup Peer Node and server is able to receive new connection.
2. Network handshake.
--------------- Architecture --------------- 
Node A                              Node B
  │                                    │
  │  listening :3000                   │
  │                                    │
  │◄────── TCP connection ─────────────│
  │                                    │
  │◄──── Node B handshake ─────────────│
  │───── Node A handshake ────────────►│
  │                                    │
  │  knows remote = node-B             │
  │                                    │
  │                         knows remote = node-A
