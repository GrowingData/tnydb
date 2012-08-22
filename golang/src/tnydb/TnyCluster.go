package tnydb

// import "bytes"
// import "encoding/gob"
import "fmt"
import "net"

type TnyCluster struct {
	ListenOn    string
	Server      *TnyServer
	Connections map[string]*TnyConnection
	ServerAddr  string
	Type        NodeType

	// Channel that enables the server to stop listening
	serving chan int

	// For blocking to wait for the network to become ready
	// to make requests too
	ready chan int
}

func NewTnyClusterClient(server_addr string) *TnyCluster {
	network := new(TnyCluster)
	network.Type = N_CLIENT
	network.ServerAddr = server_addr
	network.Connections = make(map[string]*TnyConnection)
	network.serving = make(chan int, 0)
	network.ready = make(chan int, 0)

	return network
}

func NewTnyClusterServer(me *TnyServer, listen_addr string, server_addr string) *TnyCluster {
	// Try to connect to the server specified

	network := new(TnyCluster)
	network.Type = N_SERVER
	network.ListenOn = listen_addr
	network.Server = me
	network.ServerAddr = server_addr
	network.Connections = make(map[string]*TnyConnection)
	network.serving = make(chan int, 0)
	network.ready = make(chan int, 0)

	return network

}
func (self *TnyCluster) NodeKeys() []string {
	keys := make([]string, 0)
	for k := range self.Connections {
		keys = append(keys, k)
	}
	return keys
}

func (self *TnyCluster) Start() {
	self.Listen()
	if len(self.ServerAddr) > 0 {
		self.Connect()
	}
}

func (self *TnyCluster) Listen() {
	// Listen for incoming connections
	if self.Type == N_SERVER {
		go self.BlockingListen()
	}
}

func (self *TnyCluster) Connect() {

	// fmt.Printf("Connecting to: %s... ", self.ServerAddr)

	// Join an existing network
	if socket, err := net.Dial("tcp", self.ServerAddr); err != nil {
		// Error
		fmt.Printf("Unable to connect to default server: %s\n", self.ServerAddr)

	} else {
		// Get the server, add it once the handshake is complete
		cn := ConnectionFromDial(socket, self)
		self.Connections[cn.Node.Address] = cn

		self.ConnectToCluster(cn)
	}

}

func (self *TnyCluster) WaitReady() int {
	return <-self.ready
}

func (self *TnyCluster) ConnectToCluster(connection *TnyConnection) {
	// fmt.Printf("Finding other nodes... ")
	// Send the request, and register teh following callback
	connection.Send(&(N_Topology_Request{}), func(cn_receiver *TnyConnection, packet TnyClusterPacket) {
		// fmt.Printf("Topoplogy.Response... ")

		// Callback when the response to the request has been sent to us
		if response, ok := packet.(*N_Topology_Response); !ok {
			fmt.Printf("Unable to convert packet to N_Topology_Response, ignoring request.\n")
		} else {
			dialing := make(chan int, len(response.Nodes))

			// fmt.Printf("TnyConnection.recv_loop.N_TOPOLOGY_RESPONSE: Got %d nodes.\n", len(response.Nodes))
			for _, n := range response.Nodes {
				if _, exists := self.Connections[n.Address]; n.Address != self.ListenOn && !exists {

					// Dial the other servers asynchronously
					go func(node N_NodeDetails) {
						// fmt.Printf("Dialing: %s\n", n.Address)
						if socket, err := net.Dial("tcp", n.Address); err != nil {
							fmt.Printf("Unable to connect to server: %s\n", n.Address)
						} else {
							// Get the node, add it once the handshake is complete
							cn := ConnectionFromDial(socket, self)
							self.Connections[cn.Node.Address] = cn

							dialing <- 1
						}
					}(n)
				} else {

					fmt.Printf("\tConnected to: %s (existing)\n", n.Address)
				}
			}

			// Wait for all the nodes to pick up before stating that 
			// we are ready
			for i := 0; i < len(response.Nodes); i++ {
				<-dialing
			}

			// Let everyone know that I am connected to everyone that I 
			// need to be connected to
			self.ready <- len(self.Connections)
		}
	})

}

func (self *TnyCluster) BlockingListen() {
	fmt.Printf("Listening on: %s\n", self.ListenOn)

	if netlisten, err := net.Listen("tcp", self.ListenOn); err != nil {
		fmt.Printf("Unable to listen on: %s (%s)", self.ListenOn, err.Error())
	} else {
		defer netlisten.Close()

		for {
			if socket, err := netlisten.Accept(); err != nil {
				fmt.Printf("Connection DENIED on: %s (%s)\n", self.ListenOn, err.Error())
			} else {
				fmt.Printf("Connection accepted on: %s\n", self.ListenOn)

				// Get the node, add it once the handshake is complete
				cn := ConnectionFromListen(socket, self)
				self.Connections[cn.Node.Address] = cn

			}

		}
	}
}

func (self *TnyCluster) ConnectionClosed(cn *TnyConnection) {
	fmt.Printf("Connection closed: %s\n", cn.Node.Address)
	delete(self.Connections, cn.Node.Address)

}

func (self *TnyCluster) Quit() {
	self.serving <- 1
}

func (self *TnyCluster) Wait() {
	<-self.serving
}

// Command to tell the cluster to load a database
func (self *TnyCluster) LoadDatabase(name string) *TnyDatabaseDefinition {
	fmt.Printf("> TnyCluster.LoadDatabase: \"%s\"... ", name)

	success := make(chan string, len(self.Connections))
	failed := make(chan string, len(self.Connections))
	databases := make(chan TnyDatabaseDefinition, len(self.Connections))
	firstError := true
	for k, v := range self.Connections {

		// Define a new request for each one, otherwise our Sequence Id's
		// will end up all confused
		request := N_LoadDatabase_Request{DatabaseName: name}
		v.Send(&request, func(cn *TnyConnection, packet TnyClusterPacket) {
			if response, ok := packet.(*N_LoadDatabase_Response); !ok {
				fmt.Printf("Unable to convert packet to N_LoadDatabase_Response, ignoring request.\n")
			} else {
				if len(response.Error) != 0 {
					if firstError {
						fmt.Printf("\n")
						firstError = false
					}
					fmt.Printf("\tError loading database on Node: %s (%s)\n", cn.Node.Address, response.Error)
					failed <- k
				} else {
					success <- k
					databases <- response.Database
				}
			}
		})
	}

	waitingOn := len(self.Connections)
	failed_flag := false
	for waitingOn > 0 {
		select {
		case <-success:
			waitingOn--
			// fmt.Printf("Success loading database on Node: %s, waiting on: %d\n", node, waitingOn)

		case <-failed:
			waitingOn--
			// fmt.Printf("Failed loading database on Node: %s, waiting on: %d\n", node, waitingOn)
			failed_flag = true

		}
	}
	if !failed_flag {
		fmt.Printf("Success.\n")
		database := <-databases

		return &database
	}
	// fmt.Printf("Failed.\n")

	return nil

}
