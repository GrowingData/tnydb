package tnydb

import "fmt"

var stop_fmt_error_TnyClient = fmt.Sprintf("keep 'fmt' import during debugging")

type TnyClient struct {
	Databases     map[string]*TnyDatabaseDefinition
	ServerAddress string
	Cluster       *TnyCluster
}

func NewTnyClient(server_address string) *TnyClient {
	client := new(TnyClient)

	client.Databases = make(map[string]*TnyDatabaseDefinition)
	client.ServerAddress = server_address
	client.Cluster = NewTnyClusterClient(server_address)
	client.Cluster.Connect()
	// fmt.Printf("Connecting... ")
	nodes := client.Cluster.WaitReady()

	fmt.Printf("> Connected to %d nodes\n", nodes)

	return client

}

func (self *TnyClient) LoadDatabase(database string) error {
	db := self.Cluster.LoadDatabase(database)

	if db == nil {
		return fmt.Errorf("LoadDatabase failed, returned nil\n")

	}
	self.Databases[db.Name] = db

	// fmt.Printf("LoadDatabase: %s\n", db.Name)
	return nil
}

func (self *TnyClient) GetTable(databaseName string, tableName string) (*TnyTableDefinition, error) {

	if db, ok := self.Databases[databaseName]; !ok {
		return nil, fmt.Errorf("Unable to load table. The specified database: \"%s\" has not been loaded / doesn't exist\n", databaseName)

	} else {
		var tbl *TnyTableDefinition
		for _, t := range db.Tables {
			if t.Name == tableName {
				tbl = &t
			}
		}
		if tbl == nil {
			return nil, fmt.Errorf("Unable to load table. The specified table: \"%s\" does not exist in database: \"%s\" \n", tableName, databaseName)
		} else {
			return tbl, nil
		}

	}

	// Impossible 
	return nil, nil

}

func (self *TnyClient) LoadTable(databaseName string, tableName string) error {

	if tbl, err := self.GetTable(databaseName, tableName); err != nil {
		return err
	} else {
		fmt.Printf("> Load table: \"%s\"... ", tbl.Name)
		count := 0
		nodes := len(self.Cluster.Connections)
		keys := self.Cluster.NodeKeys()

		// Limit to 1 request per node please
		// running := make(chan int, nodes)

		success := make(chan int, nodes)
		failed := make(chan int, nodes)

		for _, col := range tbl.Columns {
			// fmt.Printf("\tPageLoad.Column: %s\n", col.Name)
			for pageIdx, page := range col.Pages {

				if !page.Loaded {

					node_addr := keys[count%nodes]
					cn := self.Cluster.Connections[node_addr]

					req := N_LoadPage_Request{DatabaseName: databaseName, TableName: tableName, ColumnName: col.Name, PageIndex: pageIdx}

					// running <- 1
					cn.Send(&req, func(cn *TnyConnection, packet TnyClusterPacket) {
						if response, ok := packet.(*N_LoadPage_Response); !ok {
							fmt.Printf("Unable to convert packet to N_LoadPage_Response, ignoring request.\n")
						} else {
							if len(response.Error) > 0 {
								failed <- response.PageIndex
								// fmt.Printf("\tPageLoad.Failed(%d): %s\n", response.PageIndex, response.Error)
							} else {
								success <- 1
								// fmt.Printf("\tPageLoad.Success(%d)\n", response.PageIndex)
							}

						}
						// <-running

					})

					count++
				}
			}
		}
		waitingOn := count
		failed_flag := false
		for waitingOn > 0 {
			select {
			case <-success:
				waitingOn--
				// fmt.Printf("Success loading page\n")

			case <-failed:
				waitingOn--
				// fmt.Printf("Failed loading page\n")
				// fmt.Printf("Failed loading database on Node: %s, waiting on: %d\n", node, waitingOn)
				failed_flag = true

			}
		}

		if failed_flag {
			fmt.Printf("Failed.\n")

		} else {
			fmt.Printf("Success!\n")

		}

	}
	// Ok, everythign has been sent now, lets try checking our results
	return nil

}
