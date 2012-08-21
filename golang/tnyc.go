package main

import "fmt"
import "os"
import "tnydb"

func main() {

	if len(os.Args) < 3 {
		fmt.Printf("Usage: tnyc <server> <command> <parameters>\n E.g. tnyc 127.0.0.1:1234 load-db tester\n")
	}

	server_addr := os.Args[1]
	command := os.Args[2]

}

func initialize_network(server_addr string) tnydb.TnyServer {
	// Create a listen address...  Hmm, should the client be required to listen?
	// It would be nice if we could say that it should, but what if its stuck 
	// behind a firewall? 

	server := tnydb.NewTnyNetworkClient(server_addr)
	server.LoadDatabase()
}
