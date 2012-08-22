package tnydb

import "io"
import "encoding/gob"
import "fmt"

///////////////////////////////////////////////////////////
/////////			REQUEST
///////////////////////////////////////////////////////////
type N_LoadPage_Request struct {
	Sequence     string
	DatabaseName string
	TableName    string
	ColumnName   string
	PageIndex    int
}

func (self *N_LoadPage_Request) SetSequence(seq string) { self.Sequence = seq }
func (self *N_LoadPage_Request) GetSequence() string    { return self.Sequence }
func (self *N_LoadPage_Request) Type() RequestType      { return N_PAGE_LOAD_REQUEST }
func (self *N_LoadPage_Request) Write(conn io.Writer) error {
	return gob.NewEncoder(conn).Encode(self)
}
func (self *N_LoadPage_Request) ReadNew(conn io.Reader) (TnyClusterPacket, error) {
	var details N_LoadPage_Request
	err := gob.NewDecoder(conn).Decode(&details)
	return &details, err
}

func (self *N_LoadPage_Request) GetHandler() TnyRequestHandler {
	return func(cn *TnyConnection, packet TnyClusterPacket) {

		if req, ok := packet.(*N_LoadPage_Request); !ok {
			fmt.Printf("Unable to convert packet to N_LoadPage_Request, ignoring request.\n")
		} else {
			if cn.Cluster.Type == N_SERVER {
				// Ok, ready to roll
				response := &(N_LoadPage_Response{})
				response.SetSequence(packet.GetSequence())
				response.PageIndex = req.PageIndex
				response.Error = ""

				server := cn.Cluster.Server
				if db, ok := server.Databases[req.DatabaseName]; !ok {
					response.Error = fmt.Sprintf("LoadPage: Unable to find database \"%s\"\n", req.DatabaseName)
				} else {
					if tbl, ok := db.Tables[req.TableName]; !ok {
						response.Error = fmt.Sprintf("LoadPage: Unable to find table \"%s\"\n", req.TableName)
					} else {
						if col, ok := tbl.ColumnsMap[req.ColumnName]; !ok {
							response.Error = fmt.Sprintf("LoadPage: Unable to find column \"%s\"\n", req.ColumnName)
						} else {
							// Woohooo! We have a column
							if req.PageIndex >= len(col.PageDefinitions) || req.PageIndex < 0 {
								response.Error = fmt.Sprintf("LoadPage: PageIndex out of range \"%d\"\n", req.PageIndex)
							} else {
								pageDef := col.PageDefinitions[req.PageIndex]
								col.LoadPage(pageDef)

							}

						}

					}
				}
				// fmt.Printf("TnyConnection.N_PAGE_LOAD_REQUEST: Write: %s (type: %d)\n", response.GetSequence(), response.Type())

				cn.Transport.Out <- response
			} else {

				panic("LoadPage is not an appropriate command for Client nodes")
			}
		}

	}
}

///////////////////////////////////////////////////////////
/////////			RESPONSE
///////////////////////////////////////////////////////////
type N_LoadPage_Response struct {
	Sequence  string
	Error     string
	PageIndex int
}

func (self *N_LoadPage_Response) SetSequence(seq string) { self.Sequence = seq }
func (self *N_LoadPage_Response) GetSequence() string    { return self.Sequence }
func (self *N_LoadPage_Response) Type() RequestType      { return N_PAGE_LOAD_RESPONSE }
func (self *N_LoadPage_Response) Write(conn io.Writer) error {
	return gob.NewEncoder(conn).Encode(self)
}
func (self *N_LoadPage_Response) ReadNew(conn io.Reader) (TnyClusterPacket, error) {
	var details N_LoadPage_Response
	err := gob.NewDecoder(conn).Decode(&details)
	return &details, err
}

func (self *N_LoadPage_Response) GetHandler() TnyRequestHandler {
	return nil
}
