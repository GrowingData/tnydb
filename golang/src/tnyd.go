package main

import "fmt"
import "os"
import "bytes"
import "bufio"

// import "time"
import "runtime"
import "runtime/pprof"
import "flag"
import "log"

import "tnydb"

// import "bufio"

func main() {

	fmt.Printf("Running with %d CPU's\n", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	network_test()
	return

	server_write_test()
	server_read_test()
	return

	// start := time.Now()
	// table, err := tnydb.ReadCSV(db, filename, "quotes", 1000000)

	// // table, err := tnydb.ReadCSV(db, f, "quotes", -1)
	// nanos := time.Since(start)

	// fmt.Printf("Read took: %fms\n", float64(nanos)/1000000)

	// // table.Print()

	// if err == nil {
	// 	fmt.Printf("Read %d rows\n", table.Rows())
	// 	// return

	// } else {
	// 	fmt.Printf("Error: %s\n", err)
	// 	// return
	// }

	// // Lets profile the actual querying of data...
	// StartProfile()
	// // Do it 1000 times to make sure we capture everythingn")

	// qstart := time.Now()
	// // for i := 0; i < 1000; i++ {
	// sexp_test(db, "quotes[Date=\"2011-04-26\"]|SUM(Volume),Code")
	// // }
	// StopProfile()
	// qnanos := time.Since(qstart)
	// fmt.Printf("Query took: %fms\n", float64(qnanos)/1000000)

	// // sexp_test(db, "quotes|SUM(Volume),Code")
	return

	// Read a line, so we have the opportunity to look at 
	// how many memories are being used
	// 	var readin string 
	// 	fmt.Scanf("%s", &readin)
	// 	fmt.Printf("Error: %s\n", err)
}

func network_test() {
	listen_addr := os.Args[1]
	server_addr := ""
	if len(os.Args) > 2 {
		server_addr = os.Args[2]
	}

	server := tnydb.NewServer("Filesystem", listen_addr, server_addr)
	server.Serve()
}

func server_read_test() {

	fmt.Printf("\n\nReading...\n")
	server := tnydb.NewServer("Filesystem", os.Args[1], os.Args[2])

	db, _ := server.LoadDatabase("tester")

	fmt.Printf("Distributing pages...\n")
	server.DistributePages()

	fmt.Printf("Rows: %d, querying:\n", db.Tables["quotes"].Rows())

	// db.Tables["quotes"].Print(100)

	sexp_test(db, "quotes[Date=\"2012-03-21\"]|SUM(Volume),Code")
	// sexp_test(db, "quotes|SUM(Volume),Code")

}

func server_write_test() {

	filename := "/home/tez/data/tnydb/quotes.csv"
	server := tnydb.NewServer("Filesystem", os.Args[1], os.Args[2])
	db := server.NewDatabase("tester")

	tnydb.ReadCSV(db, filename, "quotes", -1)

	db.WriteDefinition()
	db.WriteDirtyPages()

	sexp_test(db, "quotes[Date=\"2012-03-21\"]|SUM(Volume),Code")
}

func join_test() {
	server := tnydb.NewServer("filesystem", os.Args[1], os.Args[2])
	db := server.NewDatabase("tester")

	a, err := tnydb.ReadCSV(db, "simple_test.csv", "a", 1000000)

	if err == nil {
		b, err := tnydb.ReadCSV(db, "simple_test_2.csv", "b", 1000000)
		if err == nil {
			tnydb.ProcessJoin(db, a, b, a.ColumnsMap["Code"], b.ColumnsMap["Code"]).Print(100)
		}
	}

	fmt.Println("===========WRITE TEST===============")

	page := a.Columns[0].Pages[0]
	fmt.Println(page.BitString(10))

	// Write the first page of data from simple_test
	pageDef := tnydb.TnyPageDefinition{DataPath: "output.page", Loaded: false}
	fo, err := os.Create(pageDef.DataPath)
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(fo)
	page.WritePage(writer)
	writer.Flush()
	fo.Close()

	// Now lets jsut see if we can actually read it yeah?

	fmt.Println("===========READ TEST===============")
	fi, err := os.Open("output.page")
	reader := bufio.NewReader(fi)

	readPage := tnydb.ReadPage(reader, a.Columns[0], &pageDef)

	fmt.Println(readPage.BitString(10))

}

func StopProfile() {
	pprof.StopCPUProfile()
}
func StartProfile() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}
}

func sexp_test(db *tnydb.TnyDatabase, queryString string) {

	b := bytes.NewBufferString(queryString)
	parser := tnydb.NewQueryParser(b)

	query, err := parser.ReadAll(db)
	if err != nil {
		// fmt.Printf("Parser Error: %s\n", err)
	} else {

		// fmt.Printf("\n===================\n")
		// fmt.Printf(query.ToString())
		// fmt.Printf("\nProcessing...\n")

		// start := time.Now()

		tnydb.ProcessQuery(query, db)

		// nanos := time.Since(start)

		// fmt.Printf("Query took: %fms\n", float64(nanos)/1000000)
	}

}

// func functiontester() {
// 	test := SumIntAggr()

// 	var vc tnydb.ValueContainer
// 	vc.VInt64 = 1
// 	test.Accumulate(vc)

// 	vc.VInt64 = 10
// 	test.Accumulate(vc)

// 	test.Merge(test)

// 	fmt.Printf("Final: %d\n", test.Result().VInt64)

// }
