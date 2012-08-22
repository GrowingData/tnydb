package tnydb

import "fmt"
import "runtime"
import "encoding/json"

var stop_fmt_error_TnyServer = fmt.Sprintf("keep 'fmt' import during debugging")

type TnyServer struct {
	IO            TnyIO
	Databases     map[string]*TnyDatabase
	ListenAddress string
	ServerAddress string
	Cluster       *TnyCluster
}

func NewServer(ioProvider string, listen_addr string, server_addr string) *TnyServer {
	server := new(TnyServer)
	server.Databases = make(map[string]*TnyDatabase)
	server.ListenAddress = listen_addr
	server.ServerAddress = server_addr

	// Create a server!
	server.Cluster = NewTnyClusterServer(server, listen_addr, server_addr)

	if ioProvider == "Filesystem" {
		server.IO = NewTnyIOFileSystem()
	} else {
		panic("Unknown IO provider: " + ioProvider + ". Please use \"Filesystem\".")
	}
	return server
}

func (self *TnyServer) Serve() {
	self.Cluster.Start()
	self.Cluster.Wait()

}

func (server *TnyServer) NewDatabase(name string) *TnyDatabase {
	db := new(TnyDatabase)
	db.Name = name
	db.Server = server
	db.Tables = make(map[string]*TnyTable)

	return db
}

func (self *TnyServer) LoadDatabase(name string) (*TnyDatabase, error) {

	var def TnyDatabaseDefinition

	// Load the definitiion
	filename := "db-" + name + ".json"
	if reader, err := self.IO.GetReader(filename); err != nil {
		return nil, err

	} else {
		defer self.IO.Close(filename)

		dec := json.NewDecoder(reader)
		dec.Decode(&def)

		db := new(TnyDatabase)
		db.Name = name
		db.Server = self
		db.Tables = make(map[string]*TnyTable)

		for _, t := range def.Tables {
			db.LoadTable(t)
		}

		self.Databases[name] = db

		return db, nil
	}

	return nil, nil
}

func (self *TnyServer) NodeInformation() *N_NodeDetails {
	fmt.Printf("TnyServer.NodeInformation: %s\n", self.ListenAddress)

	node := N_NodeDetails{Address: self.ListenAddress, Role: N_SERVER, CPUs: runtime.NumCPU()}
	node.Pages = make([]N_Page, 0)

	// Find my pages
	for _, d := range self.Databases {
		// fmt.Printf("Distributing to database: %s\n", d.Name)
		for _, t := range d.Tables {
			// fmt.Printf("Distributing to table: %s\n", t.Name)
			for _, c := range t.Columns {
				// fmt.Printf("Distributing to column: %s\n", c.Name)

				for _, p := range c.Pages {
					netPage := N_Page{Database: d.Name, Table: t.Name, Column: c.Name, PageIndex: p.index}
					node.Pages = append(node.Pages, netPage)
				}
			}
		}
	}
	return &node

}

func (self *TnyServer) DistributePages() {

	for _, d := range self.Databases {
		// fmt.Printf("Distributing to database: %s\n", d.Name)
		for _, t := range d.Tables {
			// fmt.Printf("Distributing to table: %s\n", t.Name)
			for _, c := range t.Columns {
				// fmt.Printf("Distributing to column: %s\n", c.Name)

				for _, p := range c.PageDefinitions {
					// fmt.Printf("Loading page: %d\n", i)
					// Now we want to allocate these to different
					// servers, but thats a bit tricky without any 
					// other servers!  So lets just load them locally
					// for now.

					c.LoadPage(p)

				}

			}
		}

	}

}
