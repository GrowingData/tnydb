package tnydb

import "encoding/binary"
import "fmt"
import "net"
import "bytes"
import "strings"

type OnConnectionClosed func()

type TnyClusterChannel struct {
	Connection *net.TCPConn
	In         chan TnyClusterPacket
	Out        chan TnyClusterPacket
	Quit       chan int
	Closed     OnConnectionClosed
}

func NewTnyClusterChannel(conn net.Conn, onClosed OnConnectionClosed) *TnyClusterChannel {
	netty := new(TnyClusterChannel)
	netty.Connection = conn.(*net.TCPConn)
	netty.Connection.SetKeepAlive(true)
	// netty.Connection.SetReadBuffer(8192)
	// netty.Connection.SetWriteBuffer(8192)
	netty.Closed = onClosed

	netty.In = make(chan TnyClusterPacket, 1)
	netty.Out = make(chan TnyClusterPacket, 0)
	netty.Quit = make(chan int, 0)

	go netty.recv_loop()
	go netty.send_loop()

	return netty
}

func (self *TnyClusterChannel) send_loop() {
	for {
		// select {
		// case packet := <-self.Out:
		packet := <-self.Out
		if err := self.WriteChunkBuffered(packet); err != nil {
			if !strings.Contains(err.Error(), "EOF") {
				fmt.Printf("WriteChunkBuffered Error: %s\n", err.Error())
			}
			self.Closed()
			break
		}
	}
}

func (self *TnyClusterChannel) recv_loop() {
	for {
		if packet, err := self.ReadChunkBuffered(); err != nil {
			self.Connection.Close()

			if err.Error() != "EOF" {
				fmt.Printf("ReadChunkBuffered Error: %s\n", err.Error())
			}

			self.Closed()
			return
		} else {
			self.In <- packet
		}
	}
}
func (self *TnyClusterChannel) WriteChunkBuffered(packet TnyClusterPacket) error {
	var buffer bytes.Buffer

	// Encode the variable
	if err := packet.Write(&buffer); err != nil {
		return fmt.Errorf("Unable to write packet data. Type: %d, Error: %s", packet.Type(), err.Error())
	} else {
		final := buffer.Bytes()

		// fmt.Printf("Write: Type: %d, Length: %d\n", packet.Type(), len(final))

		// Write the type
		if err := binary.Write(self.Connection, binary.LittleEndian, packet.Type()); err != nil {
			return fmt.Errorf("Unable to write packet type. Type: %d, Error: %s\n", packet.Type(), err.Error())
		} else {
			// Write the length of the buffer
			if err := binary.Write(self.Connection, binary.LittleEndian, uint32(len(final))); err != nil {
				return fmt.Errorf("Unable to write packet length. Length: %d, Error: %s\n", buffer.Len(), err.Error())
			} else {
				// Write the actual buffer
				if _, err := self.Connection.Write(final); err != nil {
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

func (self *TnyClusterChannel) ReadChunkBuffered() (TnyClusterPacket, error) {
	var pt RequestType
	// Read the type from the socket, so we know how to decode it
	if err := binary.Read(self.Connection, binary.LittleEndian, &pt); err != nil {
		return nil, fmt.Errorf("Unable to read packet type. Error:%s\n", err.Error())
	} else {
		// Read the length of the data from the socket, so we know when this object stops
		var length uint32
		if err := binary.Read(self.Connection, binary.LittleEndian, &length); err != nil {
			return nil, fmt.Errorf("Unable to read packet length. Error: %s\n", err.Error())
		} else {
			// fmt.Printf("Read: Type: %d, Length: %d \n", pt, length)
			// Create a buffer of "Length" and read that many bytes from the socket

			// Buffer for deserialization
			var buffer bytes.Buffer

			// Now read in chunks until its ALL read
			bytesRead := uint32(0)
			for bytesRead < length {
				bytesLeft := length - bytesRead
				if bytesLeft > 8092 {
					bytesLeft = 8092
				}
				tmpbuffer := make([]byte, bytesLeft)

				if n, err := self.Connection.Read(tmpbuffer); err != nil {
					return nil, fmt.Errorf("Unable to read buffer. Error: %s\n", err.Error())
				} else {
					bytesRead += uint32(n)
					buffer.Write(tmpbuffer[:n])
					// fmt.Printf("Read %d, cumulative: %d, total: %d\n", n, bytesRead, length)
				}

			}

			// Look up the type so we know what packet type to use to decode it
			if reader, ok := NET_REGISTERED_TYPES[pt]; !ok {
				return nil, fmt.Errorf("Unable to find decoder for PacketType: %d.\n", pt)
			} else {
				// Actually decode it
				if packet, err := reader.ReadNew(&buffer); err != nil {
					return nil, fmt.Errorf("Unable to read packet data. Type: %d. Error: %s\n", packet.Type(), err.Error())
				} else {
					// Return it, all sorted!
					return packet, nil
				}
			}

		}

	}
	return nil, fmt.Errorf("How'd we get here?  Supposed to be unreachable!")
}
