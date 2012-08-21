package tnydb

import "encoding/binary"
import "fmt"
import "net"
import "bytes"

type TnyNetworkChannel struct {
	Connection *net.TCPConn
	In         chan TnyNetworkable
	Out        chan TnyNetworkable
	Quit       chan int
}

func NewTnyNetworkChannel(conn net.Conn) *TnyNetworkChannel {
	netty := new(TnyNetworkChannel)
	netty.Connection = conn.(*net.TCPConn)
	netty.Connection.SetKeepAlive(true)
	netty.Connection.SetReadBuffer(0)
	netty.Connection.SetWriteBuffer(0)

	netty.In = make(chan TnyNetworkable, 1)
	netty.Out = make(chan TnyNetworkable, 0)
	netty.Quit = make(chan int, 0)

	go netty.recv_loop()
	go netty.send_loop()

	return netty
}

func (self *TnyNetworkChannel) send_loop() {
	for {
		// select {
		// case packet := <-self.Out:
		packet := <-self.Out
		if err := self.WriteChunkBuffered(packet); err != nil {
			fmt.Printf("WriteChunkBuffered Error: %s\n", err.Error())
			break
		}
	}
}

func (self *TnyNetworkChannel) recv_loop() {
	for {
		if packet, err := self.ReadChunkBuffered(); err != nil {
			self.Connection.Close()
			fmt.Printf("ReadChunkBuffered Error: %s\n", err.Error())
			return
		} else {
			self.In <- packet
		}
	}
}
func (self *TnyNetworkChannel) WriteChunkBuffered(packet TnyNetworkable) error {
	var buffer bytes.Buffer

	// Encode the variable
	if err := packet.Write(&buffer); err != nil {
		return fmt.Errorf("Unable to write packet data. Type: %d, Error: %s", packet.Type(), err.Error())
	} else {
		// Write the type
		if err := binary.Write(self.Connection, binary.LittleEndian, packet.Type()); err != nil {
			return fmt.Errorf("Unable to write packet type. Type: %d, Error: %s\n", packet.Type(), err.Error())
		} else {
			// Write the length of the buffer
			if err := binary.Write(self.Connection, binary.LittleEndian, uint32(buffer.Len())); err != nil {
				return fmt.Errorf("Unable to write packet length. Length: %d, Error: %s\n", buffer.Len(), err.Error())
			} else {
				// Write the actual buffer
				if _, err := self.Connection.Write(buffer.Bytes()); err != nil {
					return fmt.Errorf("Unable to write data buffer to socket. Error: %s\n", err.Error())
				} else {
					// All good!!!
					return nil
				}
			}
		}
	}
	return fmt.Errorf("How'd we get here?")
	// 

}

func (self *TnyNetworkChannel) ReadChunkBuffered() (TnyNetworkable, error) {
	var pt RequestType
	// Read the type from the socket, so we know how to decode it
	if err := binary.Read(self.Connection, binary.LittleEndian, &pt); err != nil {
		return nil, fmt.Errorf("Unable to read packet type. Error:%s", err.Error())
	} else {
		// Read the length of the data from the socket, so we know when this object stops
		var length uint32
		if err := binary.Read(self.Connection, binary.LittleEndian, &length); err != nil {
			return nil, fmt.Errorf("Unable to read packet type. Error: %s", err.Error())
		} else {
			// Create a buffer of "Length" and read that many bytes from the socket
			buffer := make([]byte, length)
			if _, err := self.Connection.Read(buffer); err != nil {
				return nil, fmt.Errorf("Unable to read packet length. Error: %s", err.Error())
			} else {
				// Look up the type so we know what packet type to use to decode it
				if reader, ok := NET_REGISTERED_TYPES[pt]; !ok {
					return nil, fmt.Errorf("Unable to find decoder for PacketType: %d. Error: %s", pt, err.Error())
				} else {
					// Actually decode it
					if packet, err := reader.ReadNew(bytes.NewBuffer(buffer)); err != nil {
						return nil, fmt.Errorf("Unable to read packet data. Type: %d. Error: %s", packet.Type(), err.Error())
					} else {
						// Return it, all sorted!
						return packet, nil
					}
				}
			}

		}

	}
	return nil, fmt.Errorf("How'd we get here?  Supposed to be unreachable!")
}
