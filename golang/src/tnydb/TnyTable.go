package tnydb

import "fmt"
import "strings"

type TnyTableDefinition struct {
	Name    string
	Columns []TnyColumnDefinition
}

type TnyTable struct {
	Name     string
	Length   int
	Database *TnyDatabase

	Columns    []*TnyColumn
	ColumnsMap map[string]*TnyColumn
}

func (self *TnyTable) NewColumn(name string, valueType ValueType) *TnyColumn {
	if self == nil {
		panic("TnyTable.NewColumn(): self==nil")
	}

	colType := GetValueType(valueType)

	col := new(TnyColumn)
	col.Type = colType
	col.Name = strings.TrimSpace(name)
	col.Table = self

	col.Pages = make([]*TnyPage, 0)
	col.NewPage()

	// col.Length = 0

	self.Columns = append(self.Columns, col)
	self.ColumnsMap[col.Name] = col

	return col
}

func (self *TnyTable) LoadColumn(def TnyColumnDefinition) *TnyColumn {
	if self == nil {
		panic("TnyTable.LoadColumn(): self==nil")
	}

	colType := GetColumnTypeFromName(def.TypeName)

	col := new(TnyColumn)
	col.Type = colType
	col.Name = def.Name

	col.Pages = make([]*TnyPage, 0)

	col.Table = self
	col.Length = def.Length

	for _, p := range def.Pages {
		// Make sure that we track that the page has not 
		// actually been loaded yet.
		p.Loaded = false
		col.PageDefinitions = append(col.PageDefinitions, p)
	}
	// Load the keys for my column yo!
	// fmt.Printf("LoadColumn: def.DataPath:%s\n", def.DataPath+".keys")
	reader, _ := self.Database.Server.IO.GetReader(def.DataPath + ".keys")
	col.ReadData(reader)

	// Now add all our references to the Table
	self.Columns = append(self.Columns, col)
	self.ColumnsMap[col.Name] = col

	return col
}

// func NewTnyTableWithColumns(db *TnyDatabase, name string, names []string, types []string) *TnyTable {
// 	tbl := db.NewTable(name)

// 	if len(names) == len(types) {

// 		for i := 0; i < len(names); i++ {
// 			typeName := strings.TrimSpace(types[i])
// 			colName := strings.TrimSpace(names[i])
// 			t := ValueTypeFromName(typeName)

// 			tbl.NewColumn(colName, t)
// 		}
// 		return tbl
// 	} else {
// 		panic("len(names)!=len(types)")
// 	}
// 	return nil

// }

func (tbl *TnyTable) Rows() int {
	// Assuming that the TnyTable has atleast 1 TnyColumn...
	return tbl.Columns[0].Length

}

func (tbl *TnyTable) PageCount() int {
	return len(tbl.Columns[0].Pages)
}

func (tbl *TnyTable) Append(row []string, index int) error {
	// fmt.Println("Appending: ", row)

	if len(row) == len(tbl.Columns) {

		// fmt.Println("Lengths match")
		for i := 0; i < len(row); i++ {
			vc, t := ParseString(row[i])
			ct := tbl.Columns[i].Type.ValueType()

			// Insert it as a NULL value
			if vc.IsMaybeNull {
				vc.IsNull = true
				tbl.Columns[i].Append(vc)
			} else {
				if t <= ct {
					tbl.Columns[i].Append(vc)

				} else {
					fmt.Printf("\nTnyColumn and CSV type missmatch at Row: %d, TnyColumn: %d.  Expected %s, Got %s. Value: '%s'\n", index, i, TnyColumnTypeLabel(ct), TnyColumnTypeLabel(t), row[i])

					// return fmt.Errorf("TnyColumn and CSV type missmatch at Row: %d, TnyColumn: %d.  Expected %s, Got %s\n", index, i, TnyColumnTypeLabel(ct), TnyColumnTypeLabel(t))
				}
			}
			// Output the value
			// fmt.Printf("%s\t", tbl.Columns[i].Access(index).ToString())
		}
		// fmt.Printf("\n")
	} else {
		return fmt.Errorf("Number of fields (%i) doesn't match number of Columns (%i)", len(tbl.Columns), len(row))
	}

	return nil
}

func (tbl *TnyTable) Print(rows int) {
	cols := len(tbl.Columns)
	for c := 0; c < cols; c++ {
		if c+1 < cols {
			fmt.Printf("%s,", tbl.Columns[c].Name)
		} else {
			fmt.Printf("%s\n", tbl.Columns[c].Name)
		}
	}

	for c := 0; c < len(tbl.Columns); c++ {
		if c+1 < cols {
			fmt.Printf("%s,", tbl.Columns[c].Type.TypeLabel())
		} else {
			fmt.Printf("%s\n", tbl.Columns[c].Type.TypeLabel())
		}
	}

	// fmt.Printf("Not implemented\n")

	// Very interesting!!! How do we handle this case for when 
	// pages are located all over the shop?
	for r := 0; r < tbl.Rows(); r++ {
		if rows != -1 && r > rows {
			break

		}

		// fmt.Printf("Row %d\n", r)
		for c := 0; c < len(tbl.Columns); c++ {
			if c+1 < cols {
				fmt.Printf("%s,", tbl.Columns[c].Access(r).Str())
			} else {
				fmt.Printf("%s\n", tbl.Columns[c].Access(r).Str())
			}
		}
	}
}

func (self *TnyTable) DataPath() string {
	if self == nil {
		panic("self is nil! TnyTable.DataPath()")
	}
	if self.Database == nil {
		panic("Database is nil! TnyTable.DataPath()")
	}

	return self.Database.DataPath() + "/" + self.Name
}

// func (self *TnyTable) LoadColumn(def TnyColumnDefinition) *TnyColumn {

// 	server := self.Database.Server
// 	reader := server.IO.GetReader(def.DataPath)

// 	// At this point we have the column.  It will have all the Keys, 
// 	// meta data and PageDefinitions but no ACTUAL pages.  They will
// 	// need to be added manually.
// 	column := ReadColumn(reader, self)
// 	return column

// }

func (self *TnyTable) GetDefinition() TnyTableDefinition {

	var def TnyTableDefinition
	def.Name = self.Name
	def.Columns = make([]TnyColumnDefinition, len(self.Columns))
	for i := 0; i < len(self.Columns); i++ {
		def.Columns[i] = self.Columns[i].GetDefinition()
	}

	return def

}
