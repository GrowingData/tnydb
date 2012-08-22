package tnydb

// import "bytes"
// import "encoding/gob"
import "fmt"
import "net"

type TnyNetwork struct {
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

func NewTnyNetworkClient(server_addr string) *TnyNetwork {
	network := new(TnyNetwork)
	network.Type = N_SERVER
	network.ServerAddr = server_addr
	network.Connections = make(map[string]*TnyConnection)
	network.serving = make(chan int, 0)
	network.ready = make(chan int, 0)

	return network
}

func NewTnyNetworkServer(me *TnyServer, listen_addr string, server_addr string) *TnyNetwork {
	// Try to connect to the server specified

	network := new(TnyNetwork)
	network.Type = N_SERVER
	network.ListenOn = listen_addr
	network.Server = me
	network.ServerAddr = server_addr
	network.Connections = make(map[string]*TnyConnection)
	network.serving = make(chan int, 0)
	network.ready = make(chan int, 0)

	return network

}
func (self *TnyNetwork) Start() {

	// Listen for incoming connections
	if self.Type == N_SERVER {
		go self.Listen()
	}

	if len(self.ServerAddr) > 0 {
		// Join an existing network
		if cn, err := net.Dial("tcp", self.ServerAddr); err != nil {
			// Error
			fmt.Printf("Unable to connect to default server: %s\n", self.ServerAddr)

		} else {
			// Get the server, add it once the handshake is complete
			server := BindConnection(cn, self)

			self.GetTopology()
		}
	} else {
		// Start a new network by just waiting

	}
}

func (self *TnyNetwork) GetTopology() {
	// Send the request, and register teh following callback
	server.Send(N_Topology_Request{}, func(packet TnyNetworkable) {

		// Callback when the response to the request has been sent to us
		if response, ok := packet.(N_Topology_Response); !ok {
			fmt.Printf("Unable to convert packet to N_Topology_Response, ignoring request.\n")
		} else {
			dialing := make(chan int, len(response.Nodes))

			fmt.Printf("TnyConnection.recv_loop.N_TOPOLOGY_RESPONSE: Got %d nodes.\n", len(response.Nodes))
			for _, n := range response.Nodes {
				if _, exists := self.Connections[n.Address]; n.Address != self.ListenOn && !exists {

					// Dial the other servers asynchronously
					go func(node N_NodeDetails) {
						fmt.Printf("Dialing: %s\n", n.Address)
						if cn, err := net.Dial("tcp", n.Address); err != nil {
							fmt.Printf("Unable to connect to server: %s\n", n.Address)
						} else {
							// Get the node, add it once the handshake is complete
							BindConnection(cn, self)
							dialing <- 1
						}
					}(n)
				}
			}
			// Wait for all the nodes to pick up before stating that 
			// we are ready
			for i := 0; i < len(response.Nodes); i++ {
				<-dialing
			}

			// Let everyone know that I am connected to everyone that I 
			// need to be connected to
			self.ready <- 1
		}
	})
}

func (self *TnyNetwork) Listen() {
	fmt.Printf("Listening on: %s\n", self.ListenOn)

	if netlisten, err := net.Listen("tcp", self.ListenOn); err != nil {
		fmt.Printf("Unable to listen on: %s (%s)", self.ListenOn, err.Error())
	} else {
		defer netlisten.Close()

		for {
			if cn, err := netlisten.Accept(); err != nil {
				fmt.Printf("Connection DENIED on: %s (%s)\n", self.ListenOn, err.Error())
			} else {
				fmt.Printf("Connection accepted on: %s\n", self.ListenOn)

				// Get the node, add it once the handshake is complete
				BindConnection(cn, self)

				cn.Send(network.Server.NodeInformation(), nil)

			}

		}
	}
}

func (self *TnyNetwork) OnNodeIdentified(cn *TnyConnection) {
	fmt.Printf("TnyNetwork.OnNodeIdentified: %s\n", cn.Node.Address)

	self.Connections[cn.Node.Address] = cn
}

func (self *TnyNetwork) Quit() {
	self.serving <- 1
}

func (self *TnyNetwork) Wait() {
	<-self.serving
}

// Command to tell the cluster to load a database
func (self *TnyNetwork) LoadDatabase(name string) {
	var request N_LoadDatabase_Request
	request.DatabaseName = name

	success := make(chan string, len(self.Connections))
	failed := make(chan string, len(self.Connections))
	for k, v := range self.Connections {
		v.Send(request, func(packet TnyNetworkable) {
			if response, ok := packet.(N_LoadDatabase_Response); !ok {
				fmt.Printf("Unable to convert packet to N_LoadDatabase_Response, ignoring request.\n")
			} else {
				if response.Error != nil {
					fmt.Printf("Error loading database on Node: %s, (%d)\n", v.Node.Address, response.Error)
					failed <- k
				} else {
					success <- k
				}
			}
		})
	}

	waitingOn = len(self.Connections)

	select {
	case node := <-success:
		fmt.Printf("Success loading database on Node: %s", node)
		waitingOn--
		if waitingOn == 0 {
			break
		}
	case node := <-failed:
		break
	}

	if waitingOn == 0 {
		fmt.Printf("Database load SUCCESS!\n")

	} else {
		fmt.Printf("Database load FAILED!\n")

	}

}
