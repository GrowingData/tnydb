package tnydb

import "fmt"
import "net"

type TnyConnection struct {
	Cluster   *TnyCluster
	Transport *TnyClusterChannel
	Node      *N_NodeDetails
	Callbacks map[string]ClusterCallback
	Quit      chan (int)
}

func ConnectionFromListen(connection net.Conn, cluster *TnyCluster) *TnyConnection {
	cn := init_connection(connection, cluster)

	// Wait for the first bit of data to be sent...
	select {
	case packet := <-cn.Transport.In:
		if node, ok := packet.(*N_NodeDetails); !ok {
			fmt.Printf("Unable to convert packet to N_NodeDetails. Hanshake Failed (Listen)!\n")

			return nil
		} else {
			// Send them a bit of information about who I am...
			nodeInfo := &(N_NodeDetails{Role: N_CLIENT})
			if cluster.Server != nil {
				nodeInfo = cluster.Server.NodeInformation()
			}
			nodeInfo.SetSequence(packet.GetSequence())
			cn.Transport.Out <- nodeInfo

			cn.Node = node

			go cn.recv_loop()

			// fmt.Printf("Connected (Listen)\n")
			return cn

		}

	}

	return nil
}

func ConnectionFromDial(connection net.Conn, cluster *TnyCluster) *TnyConnection {
	cn := init_connection(connection, cluster)
	go cn.recv_loop()

	// Dialer needs to indentify themselves...
	nodeInfo := &(N_NodeDetails{Role: N_CLIENT})
	if cluster.Server != nil {
		nodeInfo = cluster.Server.NodeInformation()
	}
	nodeInfo.SetSequence(Uuid())

	server_node := make(chan *N_NodeDetails, 1)

	cn.Send(nodeInfo, func(cn *TnyConnection, packet TnyClusterPacket) { // Callback when the response to the request has been sent to us
		if response, ok := packet.(*N_NodeDetails); !ok {
			// Error!
			server_node <- nil
		} else {
			// Got the details
			server_node <- response
		}

	})
	cn.Node = <-server_node

	if cn.Node == nil {
		fmt.Printf("Unable to convert packet to N_NodeDetails. Hanshake Failed (Dial)!\n")
		return nil
	}

	// fmt.Printf("Connected (Dialed)\n")
	return cn
}

func init_connection(connection net.Conn, cluster *TnyCluster) *TnyConnection {
	NET_INITIALIZE()

	cn := new(TnyConnection)
	cn.Transport = NewTnyClusterChannel(connection, func() { cn.Closed() })
	cn.Cluster = cluster
	cn.Callbacks = make(map[string]ClusterCallback)

	return cn
}

func (self *TnyConnection) Closed() {
	self.Cluster.ConnectionClosed(self)
	self.Quit <- 1
}

func (self *TnyConnection) Send(packet TnyClusterPacket, callback ClusterCallback) {

	if callback != nil {
		u := Uuid()
		packet.SetSequence(u)
		// fmt.Printf("TnyConnection.Sending (type: %d, seq:%s)\n", packet.Type(), packet.GetSequence())
		self.Callbacks[packet.GetSequence()] = callback
	}
	self.Transport.Out <- packet

}

func (self *TnyConnection) recv_loop() {
	for {
		select {
		case packet := <-self.Transport.In:
			pt := packet.Type()

			if reader, ok := NET_REGISTERED_TYPES[pt]; !ok {
				fmt.Printf("Unable to find decoder for PacketType: %d, ignoring.\n", pt)

			} else {
				if handler := reader.GetHandler(); handler != nil {
					handler(self, packet)
				} else {
					// No handler, so you must be a response
					if len(packet.GetSequence()) > 0 {

						if val, ok := self.Callbacks[packet.GetSequence()]; ok {
							// Awesome, we have something so lets send it to its
							// callback yeah?
							go val(self, packet)

							// Remove the reference to the callback so we don't 
							// run out of memories
							delete(self.Callbacks, packet.GetSequence())
						}
					}

				}

			}
		// The connection has broken, so get out
		case <-self.Quit:
			break

		}
	}

}
