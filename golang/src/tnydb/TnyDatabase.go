package tnydb

import "encoding/json"

type TnyDatabaseDefinition struct {
	Name   string
	Tables []TnyTableDefinition
}
type TnyDatabase struct {
	Name   string
	Server *TnyServer
	Tables map[string]*TnyTable
}

func (self *TnyDatabase) DataPath() string {
	return self.Name
}

func (self *TnyDatabase) NewTable(name string) *TnyTable {
	tbl := new(TnyTable)
	if len(name) == 0 {
		name = Uuid()
	}

	tbl.Name = name
	tbl.Database = self

	tbl.Columns = make([]*TnyColumn, 0)
	tbl.ColumnsMap = make(map[string]*TnyColumn)

	self.Tables[tbl.Name] = tbl

	return tbl
}
func (self *TnyDatabase) LoadTable(def TnyTableDefinition) *TnyTable {
	tbl := new(TnyTable)
	tbl.Name = def.Name
	tbl.Database = self

	tbl.Columns = make([]*TnyColumn, 0)
	tbl.ColumnsMap = make(map[string]*TnyColumn)

	for _, c := range def.Columns {
		tbl.LoadColumn(c)
	}

	self.Tables[tbl.Name] = tbl
	return tbl

}

func (self *TnyDatabase) GetDefinition() TnyDatabaseDefinition {

	var def TnyDatabaseDefinition
	def.Name = self.Name
	def.Tables = make([]TnyTableDefinition, len(self.Tables))
	i := 0
	for _, v := range self.Tables {
		// for i := 0; i < len(self.Tables); i++ {
		def.Tables[i] = v.GetDefinition()
		i++
	}

	return def
}

func (self *TnyDatabase) WriteDefinition() {
	def := self.GetDefinition()

	filename := "db-" + self.Name + ".json"

	writer := self.Server.IO.GetWriter(filename)
	defer self.Server.IO.Close(filename)

	enc := json.NewEncoder(writer)
	enc.Encode(def)
}

func (self *TnyDatabase) WriteDirtyPages() {

	// max_writers := 20
	// doneski := make(chan int, max_writers)

	waitingOn := 0
	for _, tbl := range self.Tables {
		for _, col := range tbl.Columns {
			has_dirty := false
			for _, p := range col.Pages {

				// Write the file asynchronously
				if p.dirty {
					has_dirty = true
					waitingOn++
					page := p

					// go func(page *TnyPage) {
					writer := self.Server.IO.GetWriter(page.DataPath())
					page.WritePage(writer)
					self.Server.IO.Close(page.DataPath())

					// doneski <- 1
					// }(p)

				}
			}
			if has_dirty {
				// Write the keys for this page
				writer := self.Server.IO.GetWriter(col.DataPath() + ".keys")
				col.WriteData(writer)
				self.Server.IO.Close(col.DataPath())

			}

		}
	}
	// Wait for all the writers to finish
	// for i := 0; i < waitingOn; i++ {
	// 	<-doneski
	// }
}
