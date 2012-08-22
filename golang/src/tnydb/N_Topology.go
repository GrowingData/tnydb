package tnydb

import "io"
import "encoding/gob"
import "fmt"

///////////////////////////////////////////////////////////
/////////			REQUEST
///////////////////////////////////////////////////////////
type N_Topology_Request struct {
	Sequence string
}

func (self *N_Topology_Request) SetSequence(seq string)     { self.Sequence = seq }
func (self *N_Topology_Request) GetSequence() string        { return self.Sequence }
func (self *N_Topology_Request) Type() RequestType          { return RequestType(N_TOPOLOGY_REQUEST) }
func (self *N_Topology_Request) Write(conn io.Writer) error { return gob.NewEncoder(conn).Encode(self) }
func (self *N_Topology_Request) ReadNew(conn io.Reader) (TnyClusterPacket, error) {
	var details N_Topology_Request
	err := gob.NewDecoder(conn).Decode(&details)
	return &details, err
}

func (self *N_Topology_Request) GetHandler() TnyRequestHandler {
	return func(cn *TnyConnection, packet TnyClusterPacket) {
		if _, ok := packet.(*N_Topology_Request); !ok {
			fmt.Printf("Unable to convert packet to N_Topology_Request, ignoring request.\n")
		} else {

			response := &(N_Topology_Response{})
			for _, cn := range cn.Cluster.Connections {
				if cn.Node.Role == N_SERVER {
					response.Nodes = append(response.Nodes, *cn.Node)
					fmt.Printf("\tNode: %s\n", cn.Node.Address)
				}
			}
			response.SetSequence(packet.GetSequence())

			// fmt.Printf("TnyConnection.N_TOPOLOGY_REQUEST: Write: %s\n", response.GetSequence())
			cn.Transport.Out <- response
		}
	}
}

///////////////////////////////////////////////////////////
/////////			RESPONSE
///////////////////////////////////////////////////////////
type N_Topology_Response struct {
	Sequence string
	Nodes    []N_NodeDetails
}

func (self *N_Topology_Response) SetSequence(seq string)     { self.Sequence = seq }
func (self *N_Topology_Response) GetSequence() string        { return self.Sequence }
func (self *N_Topology_Response) Type() RequestType          { return RequestType(N_TOPOLOGY_RESPONSE) }
func (self *N_Topology_Response) Write(conn io.Writer) error { return gob.NewEncoder(conn).Encode(self) }
func (self *N_Topology_Response) ReadNew(conn io.Reader) (TnyClusterPacket, error) {
	var details N_Topology_Response
	err := gob.NewDecoder(conn).Decode(&details)
	return &details, err
}

func (self *N_Topology_Response) GetHandler() TnyRequestHandler {
	return nil
}
