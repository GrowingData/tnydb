package tnydb

import "os"
import "encoding/csv"
import "fmt"
import "io"

// Reads a CSV file into a TnyTable...

func ReadCSV(db *TnyDatabase, filename string, name string, rows int) (*TnyTable, error) {
	f, err := os.Open(filename)

	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return nil, err
	}

	return readFile(db, f, name, rows)

}

func readFile(db *TnyDatabase, reader io.Reader, name string, rows int) (*TnyTable, error) {

	r := csv.NewReader(reader)

	// Read the first 2 lines to get the names and types
	names, err := r.Read()
	if err != nil {
		fmt.Printf("Reading names failed\n")
		return nil, err
	}
	// fmt.Println(names)

	types, err := r.Read()
	if err != nil {
		fmt.Printf("Reading types failed\n")
		return nil, err
	}

	// fmt.Println(types)

	table := db.NewTable(name)

	for i, _ := range names {
		table.NewColumn(names[i], ValueTypeFromName(types[i]))
	}
	// fmt.Println("TnyTable built!", TnyTable)

	count := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			return table, nil
		}

		if err != nil {
			return nil, err
		}

		parseError := table.Append(record, count)
		if parseError != nil {
			return nil, err
		}
		count++

		if rows != -1 && rows <= count {
			break
		}
	}

	return table, nil
}
