package tnydb

import "io"
import "encoding/gob"

type N_NodeDetails struct {
	Sequence string
	Address  string
	Role     NodeType
	Pages    []N_Page
	CPUs     int
}

func (self *N_NodeDetails) SetSequence(seq string)     { self.Sequence = seq }
func (self *N_NodeDetails) GetSequence() string        { return self.Sequence }
func (self *N_NodeDetails) Type() RequestType          { return RequestType(N_IAM) }
func (self *N_NodeDetails) Write(conn io.Writer) error { return gob.NewEncoder(conn).Encode(self) }
func (self *N_NodeDetails) ReadNew(conn io.Reader) (TnyClusterPacket, error) {
	var details N_NodeDetails
	err := gob.NewDecoder(conn).Decode(&details)
	return &details, err
}

func (self *N_NodeDetails) GetHandler() TnyRequestHandler {
	return nil
}
