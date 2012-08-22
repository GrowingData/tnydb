package tnydb

import "io"

type RequestType uint32

type NodeType uint32

const (
	N_SERVER NodeType = iota // Holds pages of data and responds to commands
	N_CLIENT NodeType = iota // Just issues commands without holding pages
)

type ClusterCallback func(cn *TnyConnection, packet TnyClusterPacket)

var NET_REGISTERED_TYPES map[RequestType]TnyClusterPacket = make(map[RequestType]TnyClusterPacket)

func NET_INITIALIZE() {
	nodeDetails := N_NodeDetails{}
	NET_REGISTERED_TYPES[(&nodeDetails).Type()] = &nodeDetails

	topologyRequest := N_Topology_Request{}
	NET_REGISTERED_TYPES[(&topologyRequest).Type()] = &topologyRequest

	topologyResponse := N_Topology_Response{}
	NET_REGISTERED_TYPES[(&topologyResponse).Type()] = (&topologyResponse)

	loadDbRequest := N_LoadDatabase_Request{}
	NET_REGISTERED_TYPES[(&loadDbRequest).Type()] = (&loadDbRequest)

	loadDbResponse := N_LoadDatabase_Response{}
	NET_REGISTERED_TYPES[(&loadDbResponse).Type()] = (&loadDbResponse)

	loadPageRequest := N_LoadPage_Request{}
	NET_REGISTERED_TYPES[(&loadPageRequest).Type()] = (&loadPageRequest)

	loadPageResponse := N_LoadPage_Response{}
	NET_REGISTERED_TYPES[(&loadPageResponse).Type()] = (&loadPageResponse)
}

type N_Page struct {
	Sequence  string
	Database  string
	Table     string
	Column    string
	PageIndex int
}

const (
	N_QUIT RequestType = iota
	N_IAM
	N_TOPOLOGY_REQUEST
	N_TOPOLOGY_RESPONSE

	N_DATABASE_LOAD_REQUEST
	N_DATABASE_LOAD_RESPONSE

	N_PAGE_LOAD_REQUEST
	N_PAGE_LOAD_RESPONSE
)

type TnyRequestHandler func(cn *TnyConnection, packet TnyClusterPacket)

type TnyClusterPacket interface {
	GetSequence() string
	SetSequence(seq string)
	Type() RequestType
	Write(conn io.Writer) error
	ReadNew(conn io.Reader) (TnyClusterPacket, error)
	GetHandler() TnyRequestHandler
}
