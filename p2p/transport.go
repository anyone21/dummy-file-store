package p2p

// Peer is an interface that represent the remote node
type Peer interface {
	ID() string
}

// Transport is anything that handles the communication
// between the nodes in the network. This can be of the form
// (TCP, UDP, websockets....)
type Transport interface {
	ListenAndAccept() error // Depending on whatever kind of Transport we are having we are gone called ListenAndAccept()
}
