package tnydb

import "fmt"
import "net"

type TnyConnection struct {
	Network   *TnyNetwork
	Transport *TnyNetworkChannel
	Node      N_NodeDetails
	Callbacks map[string]NetworkCallback
}

// Create a new TnyConnection for a new node entering the ystem
func BindConnection(connection net.Conn, network *TnyNetwork) *TnyConnection {
	node := new(TnyConnection)
	node.Transport = NewTnyNetworkChannel(connection)
	node.Network = network

	go node.recv_loop()

	// Tell them who we are
	node.Transport.Out <- network.Server.NodeInformation()
	return node
}

func (self *TnyConnection) RequestTopology() {
	// Ask them who they know about
	self.Transport.Out <- N_Topology_Request{}
}

func (self *TnyConnection) Send(packet TnyNetworkable, callback NetworkCallback) {
	if callback != nil {
		packet.SetSequence(Uuid())
		self.Callbacks[packet.GetSequence()] = callback
	}
	node.Transport.Out <- packet
}

func (self *TnyConnection) recv_loop() {
	for {
		select {
		case packet := <-self.Transport.In:
			// When a new packet arrives, decide what to do with it
			switch packet.Type() {

			// Requests made by 
			case N_IAM:
				fmt.Printf("TnyConnection.recv_loop.N_IAM\n")
				if node, ok := packet.(N_NodeDetails); !ok {
					fmt.Printf("Unable to convert packet to N_NodeDetails, ignoring request.\n")
				} else {
					self.Node = node
					self.Network.OnNodeIdentified(self)
				}

			case N_TOPOLOGY_REQUEST:
				fmt.Printf("TnyConnection.recv_loop.N_TOPOLOGY_REQUEST: Sending %d nodes.\n", len(self.Network.Connections))
				topology := N_Topology_Response{}
				for _, cn := range self.Network.Connections {
					topology.Nodes = append(topology.Nodes, cn.Node)
					fmt.Printf("\tNode: %s\n", cn.Node.Address)
				}
				topology.SetSequence(packet.GetSequence())
				self.Transport.Out <- topology

			case N_LoadDatabase_Request:
				fmt.Printf("TnyConnection.recv_loop.N_LoadDatabase_Request:\n")
				if req, ok := packet.(N_LoadDatabase_Request); !ok {
					fmt.Printf("Unable to convert packet to N_LoadDatabase_Request, ignoring request.\n")
				} else {
					self.Network.Server.LoadDatabase(req.DatabaseName)

				}

			// Responses to requests
			default:

				// The packet is a response to something that we already sent
				if packet.GetSequence() != nil {

					if val, ok := self.Callbacks[packet.GetSequence()]; ok {
						// Awesome, we have something so lets send it to its
						// callback yeah?
						go val(packet)

						// Remove the reference to the callback so we don't 
						// run out of memories
						delete(self.Callbacks, packet.GetSequence())
					}
				}

				// // What other nodes are in the network?

				// // Here are all the nodes I know about
				// case N_TOPOLOGY_RESPONSE:
				// 	if response, ok := packet.(N_Topology_Response); !ok {
				// 		fmt.Printf("Unable to convert packet to N_Topology_Response, ignoring request.\n")
				// 	} else {
				// 		fmt.Printf("TnyConnection.recv_loop.N_TOPOLOGY_RESPONSE: Got %d nodes.\n", len(response.Nodes))
				// 		self.Network.OnTopologyReceived(self, response.Nodes)
				// 	}
				// case N_DATABASE_LOAD_REQUEST:
				// 	if response, ok := packet.(N_LoadDatabase_Request); !ok {
				// 		fmt.Printf("Unable to convert packet to N_LoadDatabase_Request, ignoring request.\n")
				// 	} else {
				// 		self.Network.Server.LoadDatabase(response.DatabaseName, response.DistributePages)
				// 	}

				// Now somehow we need to 
			}

		}
	}

}
