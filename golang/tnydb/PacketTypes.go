package tnydb

import "io"
import "encoding/gob"

//////////////////////////////////////////////////////////////////////////////////////////
type N_NodeDetails struct {
	Sequence   string
	Address    string
	Capability NodeType
	Pages      []N_Page
}

func (self N_NodeDetails) SetSequence(seq string)     { self.Sequence = seq }
func (self N_NodeDetails) GetSequence() string        { return self.Sequence }
func (self N_NodeDetails) Type() RequestType          { return RequestType(N_IAM) }
func (self N_NodeDetails) Write(conn io.Writer) error { return gob.NewEncoder(conn).Encode(self) }
func (self N_NodeDetails) ReadNew(conn io.Reader) (TnyNetworkable, error) {
	var details N_NodeDetails
	err := gob.NewDecoder(conn).Decode(&details)
	return details, err
}

//////////////////////////////////////////////////////////////////////////////////////////
type N_Topology_Request struct {
	Sequence string
}

func (self N_Topology_Request) SetSequence(seq string)     { self.Sequence = seq }
func (self N_Topology_Request) GetSequence() string        { return self.Sequence }
func (self N_Topology_Request) Type() RequestType          { return RequestType(N_TOPOLOGY_REQUEST) }
func (self N_Topology_Request) Write(conn io.Writer) error { return gob.NewEncoder(conn).Encode(self) }
func (self N_Topology_Request) ReadNew(conn io.Reader) (TnyNetworkable, error) {
	var details N_Topology_Request
	err := gob.NewDecoder(conn).Decode(&details)
	return details, err
}

//////////////////////////////////////////////////////////////////////////////////////////
type N_Topology_Response struct {
	Sequence string
	Nodes    []N_NodeDetails
}

func (self N_Topology_Response) SetSequence(seq string)     { self.Sequence = seq }
func (self N_Topology_Response) GetSequence() string        { return self.Sequence }
func (self N_Topology_Response) Type() RequestType          { return RequestType(N_TOPOLOGY_RESPONSE) }
func (self N_Topology_Response) Write(conn io.Writer) error { return gob.NewEncoder(conn).Encode(self) }
func (self N_Topology_Response) ReadNew(conn io.Reader) (TnyNetworkable, error) {
	var details N_Topology_Response
	err := gob.NewDecoder(conn).Decode(&details)
	return details, err
}

//////////////////////////////////////////////////////////////////////////////////////////
type N_LoadDatabase_Request struct {
	Sequence     string
	DatabaseName string
}

func (self N_LoadDatabase_Request) SetSequence(seq string) { self.Sequence = seq }
func (self N_LoadDatabase_Request) GetSequence() string    { return self.Sequence }
func (self N_LoadDatabase_Request) Type() RequestType      { return RequestType(N_DATABASE_LOAD_REQUEST) }
func (self N_LoadDatabase_Request) Write(conn io.Writer) error {
	return gob.NewEncoder(conn).Encode(self)
}
func (self N_Topology_Request) ReadNew(conn io.Reader) (TnyNetworkable, error) {
	var details N_LoadDatabase_Request
	err := gob.NewDecoder(conn).Decode(&details)
	return details, err
}

//////////////////////////////////////////////////////////////////////////////////////////
type N_LoadDatabase_Response struct {
	Sequence string
	Error    string
}

func (self N_LoadDatabase_Response) SetSequence(seq string) { self.Sequence = seq }
func (self N_LoadDatabase_Response) GetSequence() string    { return self.Sequence }
func (self N_LoadDatabase_Response) Type() RequestType      { return RequestType(N_DATABASE_LOAD_RESPONSE) }
func (self N_LoadDatabase_Response) Write(conn io.Writer) error {
	return gob.NewEncoder(conn).Encode(self)
}
func (self N_Topology_Request) ReadNew(conn io.Reader) (TnyNetworkable, error) {
	var details N_LoadDatabase_Response
	err := gob.NewDecoder(conn).Decode(&details)
	return details, err
}
