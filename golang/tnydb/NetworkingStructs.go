package tnydb

import "io"

type RequestType uint32

const (
	N_QUIT RequestType = iota
	N_IAM
	N_TOPOLOGY_REQUEST
	N_TOPOLOGY_RESPONSE

	N_DATABASE_LOAD_REQUEST
	N_DATABASE_LOAD_RESPONSE
)

type NodeType uint32

const (
	N_SERVER NodeType = iota // Holds pages of data and responds to commands
	N_CLIENT NodeType = iota // Just issues commands without holding pages
)

type NetworkCallback func(packet TnyNetworkable)

var NET_REGISTERED_TYPES map[RequestType]TnyNetworkable = make(map[RequestType]TnyNetworkable)

func NET_INITIALIZE() {
	NET_REGISTERED_TYPES[N_NodeDetails{}.Type()] = N_NodeDetails{}
	NET_REGISTERED_TYPES[N_Topology_Request{}.Type()] = N_Topology_Request{}
	NET_REGISTERED_TYPES[N_Topology_Response{}.Type()] = N_Topology_Response{}
}

type TnyNetworkable interface {
	GetSequence() string
	SetSequence(seq string)
	Type() RequestType
	Write(conn io.Writer) error
	ReadNew(conn io.Reader) (TnyNetworkable, error)
}

type N_Page struct {
	Sequence  string
	Database  string
	Table     string
	Column    string
	PageIndex int
}
