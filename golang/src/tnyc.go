package main

import "fmt"
import "os"
import "tnydb"

func main() {

	if len(os.Args) < 3 {
		fmt.Printf("Usage: tnyc <server> <command> <parameters>\n E.g. tnyc 127.0.0.1:1234 load-db tester\n")
		return
	}

	server_addr := os.Args[1]

	client := tnydb.NewTnyClient(server_addr)

	// params := os.Args[2:]
	// handle_command(params, client)

	if err := client.LoadDatabase("tester"); err != nil {
		fmt.Printf("LoadDatabase Error: %s\n", err.Error())
	} else {
		if err := client.LoadTable("tester", "quotes"); err != nil {
			fmt.Printf("LoadTable Error: %s\n", err.Error())
		} else {

		}
	}
	// // Don't disconnect
	client.Cluster.Wait()

}
func handle_command(params []string, client *tnydb.TnyClient) {

	cmd := params[0]
	switch cmd {
	case "load_db":
		client.LoadDatabase(params[1])
	}

}
