package tnydb

// #cgo CFLAGS: -std=gnu99 -msse4.1 -I../../c/core/
// #cgo LDFLAGS: -L../../Debug/ -ltnydb
// #include "tny_page.h"
// #include "tny.h"
import "C"
import "fmt"
import "bufio"

// import "encoding/binary"

// import "encoding/json"

const TnyPage_LENGTH = 8192

type TnyColumnDefinition struct {
	Name     string
	TypeName string
	DataPath string
	Length   int
	Pages    []TnyPageDefinition
}

type TnyColumn struct {
	Type            TnyColumnType
	Length          int
	Name            string
	Table           *TnyTable
	Pages           []*TnyPage
	PageDefinitions []TnyPageDefinition
}

var stop_fmt_error_TnyColumn = fmt.Sprintf("keep 'fmt' import during debugging")

// Adds a value to the TnyColumn
func (col *TnyColumn) Append(value ValueContainer) {

	// NULL values always have a key index of Zero

	idx := 0
	if !value.IsNull {
		key_idx, found := col.Type.FindKey(value)
		if !found {
			new_idx := col.Type.InsertKey(value)
			idx = new_idx
		} else {
			idx = key_idx
		}
	}

	// There should always be atleast 1 page
	if len(col.Pages) == 0 {
		panic("Improperly initialized TnyColumn, len(Pages)==0")
	}

	page := col.Pages[len(col.Pages)-1]
	if page.Length() >= TnyPage_LENGTH-1 {
		page = col.NewPage()
	}
	col.Length++
	page.Append(idx)
}

func (col *TnyColumn) GetTnyPage(page_idx int) *TnyPage {
	if page_idx < len(col.Pages) {
		return col.Pages[page_idx]
	}
	return nil

}

// Get the value at the specified index within the TnyColumn
func (col *TnyColumn) Access(idx int) ValueContainer {
	page_idx := idx / TnyPage_LENGTH
	TnyPage_value_idx := idx % TnyPage_LENGTH

	key_index := col.Pages[page_idx].Access(TnyPage_value_idx)

	// fmt.Printf("Access {page_idx: %d, TnyPage_value_idx: %d, key_index: %d\n", page_idx, TnyPage_value_idx, key_index)

	return col.Type.KeyAt(key_index)
}

func (self *TnyColumn) DataPath() string {
	if self == nil {
		panic("TnyColumn.DataPath(): self==nil")
	}
	if self.Table == nil {
		panic("TnyColumn.DataPath(): self.Table==nil")
	}

	return self.Table.DataPath() + "/" + self.Name
}

func (self *TnyColumn) GetDefinition() TnyColumnDefinition {
	var def TnyColumnDefinition
	def.Name = self.Name
	def.TypeName = self.Type.TypeLabel()
	def.DataPath = self.DataPath()
	def.Length = self.Length
	def.Pages = self.PageDefinitions

	return def
}

// Write the columns data to a file, including Keys and references to Pages
func (self *TnyColumn) WriteData(writer *bufio.Writer) {
	// Write the actual keys yeah!
	self.Type.Write(writer)
	writer.Flush()
}

// Read the output from teh reader and return the Column with everything
// all nicely sorted out.
func (self *TnyColumn) ReadData(reader *bufio.Reader) {
	// Read it all it all!

	// Read the actual keys for this column!
	self.Type.Read(reader)

	// Load up the Page definitions, but don't actually load the pages?
	// Loadting the page deinfitions in memory is probably a good idea
	// as it will make it much easier to load pages, and it will also
	// give the system some visibility in terms of what pages are available
	// even if they dont exist on this Node.

}

// Managing serialization of Pages
func (self *TnyColumn) NewPage() *TnyPage {
	C_key_count := C.int(self.Type.KeyCount())

	page := new(TnyPage)
	page.Column = self
	page.index = len(self.Pages)
	// Also allocate the page_data
	page.cptr = C.tny_page_new(C_key_count, C.int(0))

	self.Pages = append(self.Pages, page)
	self.PageDefinitions = append(self.PageDefinitions, page.GetDefinition())

	return page
}

func (self *TnyColumn) KeyCount() int {
	return self.Type.KeyCount()
}

func (self *TnyColumn) LoadPage(def TnyPageDefinition) *TnyPage {

	server := self.Table.Database.Server

	reader := server.IO.GetReader(def.DataPath)
	defer server.IO.Close(def.DataPath)

	page := ReadPage(reader, self)

	self.Pages = append(self.Pages, page)

	return page

}

func (self *TnyColumn) SavePage(index int) TnyPageDefinition {

	server := self.Table.Database.Server
	page := self.Pages[index]
	def := page.GetDefinition()

	writer := server.IO.GetWriter(def.DataPath)

	page.WritePage(writer)

	return def

}
