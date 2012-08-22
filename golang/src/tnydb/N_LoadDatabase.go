package tnydb

import "io"
import "encoding/gob"
import "fmt"

///////////////////////////////////////////////////////////
/////////			REQUEST
///////////////////////////////////////////////////////////
type N_LoadDatabase_Request struct {
	Sequence     string
	DatabaseName string
}

func (self *N_LoadDatabase_Request) SetSequence(seq string) { self.Sequence = seq }
func (self *N_LoadDatabase_Request) GetSequence() string    { return self.Sequence }
func (self *N_LoadDatabase_Request) Type() RequestType      { return RequestType(N_DATABASE_LOAD_REQUEST) }
func (self *N_LoadDatabase_Request) Write(conn io.Writer) error {
	return gob.NewEncoder(conn).Encode(self)
}
func (self *N_LoadDatabase_Request) ReadNew(conn io.Reader) (TnyClusterPacket, error) {
	var details N_LoadDatabase_Request
	err := gob.NewDecoder(conn).Decode(&details)
	return &details, err
}

func (self *N_LoadDatabase_Request) GetHandler() TnyRequestHandler {
	return func(cn *TnyConnection, packet TnyClusterPacket) {

		if req, ok := packet.(*N_LoadDatabase_Request); !ok {
			fmt.Printf("Unable to convert packet to N_LoadDatabase_Request, ignoring request.\n")
		} else {
			if cn.Cluster.Type == N_SERVER {

				response := &(N_LoadDatabase_Response{})
				response.SetSequence(packet.GetSequence())

				if database, err := cn.Cluster.Server.LoadDatabase(req.DatabaseName); err != nil {
					response.Error = err.Error()
				} else {
					response.Database = database.GetDefinition()
				}
				// fmt.Printf("TnyConnection.N_DATABASE_LOAD_REQUEST: Write: %s\n", response.GetSequence())
				cn.Transport.Out <- response
			} else {
				panic("LoadDatabase is not an appropriate command for Client nodes")
			}
		}

	}
}

///////////////////////////////////////////////////////////
/////////			RESPONSE
///////////////////////////////////////////////////////////
type N_LoadDatabase_Response struct {
	Sequence string
	Error    string
	Database TnyDatabaseDefinition
}

func (self *N_LoadDatabase_Response) SetSequence(seq string) { self.Sequence = seq }
func (self *N_LoadDatabase_Response) GetSequence() string    { return self.Sequence }
func (self *N_LoadDatabase_Response) Type() RequestType      { return RequestType(N_DATABASE_LOAD_RESPONSE) }
func (self *N_LoadDatabase_Response) Write(conn io.Writer) error {
	return gob.NewEncoder(conn).Encode(self)
}
func (self *N_LoadDatabase_Response) ReadNew(conn io.Reader) (TnyClusterPacket, error) {
	var details N_LoadDatabase_Response
	err := gob.NewDecoder(conn).Decode(&details)
	return &details, err
}

func (self *N_LoadDatabase_Response) GetHandler() TnyRequestHandler {
	return nil
}
