package tnydb

// #cgo CFLAGS: -std=gnu99 -msse4.1 -I../../../c/
// #cgo LDFLAGS: -L../../lib/ -ltnydb
// #include "tny_page.h"
// #include "tny.h"
import "C"
import "unsafe"
import "reflect"
import "bufio"
import "encoding/binary"

import "fmt"

const PAGE_MAX_VALUES = 8192

var place_holder_to_stop_fmt_error = fmt.Sprintf("keep 'fmt' import during debugging")

// This is the structure that we use to store information about the
// Page when we save the database.  We also use it to send to the client
// on "load-batabase" so it can instruct the cluster on what pages to load

type TnyPageDefinition struct {
	DataPath string
	Loaded   bool
}

type TnyPage struct {
	Column *TnyColumn
	dirty  bool
	index  int
	cptr   *_Ctype_tny_page
}

type TnyPageValueList struct {
	cptr   *_Ctype_int
	Values []int
}

func (self *TnyPage) Length() int {
	return int(self.cptr.length)
}

func (self *TnyPage) Depth() int {
	return int(self.cptr.depth)
}

func (self *TnyPage) DataPath() string {
	if self == nil {
		panic("TnyPage.DataPath(): self==nil")
	}
	if self.Column == nil {
		panic("TnyPage.DataPath(): self.Column==nil")
	}

	formatted := ""
	if self.index < 10 {
		formatted = "00000" + fmt.Sprintf("%d", self.index)
	} else if self.index < 100 {
		formatted = "0000" + fmt.Sprintf("%d", self.index)
	} else if self.index < 1000 {
		formatted = "000" + fmt.Sprintf("%d", self.index)
	} else if self.index < 10000 {
		formatted = "00" + fmt.Sprintf("%d", self.index)
	} else if self.index < 100000 {
		formatted = "0" + fmt.Sprintf("%d", self.index)
	} else {
		formatted = "" + fmt.Sprintf("%d", self.index)
	}

	return fmt.Sprintf(self.Column.DataPath()+"/page-%s.page", formatted)
}

func (self *TnyPage) GetDefinition() TnyPageDefinition {
	var def TnyPageDefinition
	def.DataPath = self.DataPath()
	def.Loaded = true

	return def
}

func (pvl TnyPageValueList) Free() {
	C.tny_page_array_free(pvl.cptr, C.int(len(pvl.Values)))
}

// Writes the tny_page to the specified writer.  We want to do this
// in Go so that we can write a page to local disk, Hadoop, S3, etc
func (self *TnyPage) WritePage(writer *bufio.Writer) {
	// Lets write out the page...

	// fmt.Printf("Writing page for %s (ptr: %p, len: %d):\n", self.Column.Name, self.cptr, self.Length())

	if err := binary.Write(writer, binary.LittleEndian, int32(self.Depth())); err != nil {
		panic("Unable to write page header (depth) (" + err.Error() + ")")
	}

	if err := binary.Write(writer, binary.LittleEndian, int32(self.Length())); err != nil {
		panic("Unable to write page header (length) (" + err.Error() + ")")
	}

	// fmt.Printf("Wrote header, writing data now...\n")

	for d := 0; d < int(self.Depth()); d++ {
		bmp := self.BitmapAtDepth(d)
		bmpSlice := bmp.Slice()

		// fmt.Printf("\t%s\n", bmp.BitString(100))

		// fmt.Printf("Writing slice %d...\n", d)

		if err := binary.Write(writer, binary.LittleEndian, bmpSlice); err != nil {
			panic("Unable to write page (" + err.Error() + ")")
		}

	}
	// fmt.Printf("Write Page (%d): ", depth)
	// for i := 0; i < 10; i++ {
	// 	fmt.Printf("%d, ", self.Access(i))
	// }
	// fmt.Printf("\n")
}

func ReadPage(reader *bufio.Reader, column *TnyColumn, def *TnyPageDefinition) *TnyPage {

	C_key_count := C.int(column.KeyCount())

	page := new(TnyPage)
	page.Column = column
	page.dirty = false

	var depth, length int32

	binary.Read(reader, binary.LittleEndian, &depth)
	binary.Read(reader, binary.LittleEndian, &length)

	page.cptr = C.tny_page_new(C_key_count, C.int(length))

	// fmt.Printf("Reading page for %s (ptr: %p, len: %d):\n", page.Column.Name, page.cptr, page.Length())

	// C_depth := C.tny_page_depth(page.cptr)

	// fmt.Printf("Reading page for %s (%p):\n", column.Name, page.cptr)
	// Allocate the page in C

	for d := int32(0); d < depth; d++ {
		// Get C to make us a Bitmap so that its aligned and not
		// garbage collected

		// Get the allocated line and make a slice pointing to it that
		// can be updated by the binary.Read
		bmp := page.BitmapAtDepth(int(d))

		// We want a slice that we can pass to binary.Read, but we need
		// to make sure that its not going to get collected, so check this out!

		word_length := (PAGE_MAX_VALUES / 64)

		// Make a slice, then record its header
		bmpSlice := make([]uint64, 1)

		sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&bmpSlice)))

		// Track what it was, so we can set it back and GC can clean that up
		// rather than trashing my actual data
		o_cap := sliceHeader.Cap
		o_len := sliceHeader.Len
		o_data := sliceHeader.Data

		sliceHeader.Cap = (word_length)
		sliceHeader.Len = (word_length)
		sliceHeader.Data = uintptr(unsafe.Pointer(bmp.cptr))

		// fmt.Printf("Pointer pre: %p\n", SlicePointer(bmpSlice))
		// fmt.Printf("Reading data for depth %d...\n", d)
		// This should read my data straight into my C array!
		// Unless it doesnt work ofcourse!

		// Read data from the file into my C array
		err := binary.Read(reader, binary.LittleEndian, bmpSlice)

		// Now that we have read my data, reset the slice back to what it was
		// ready for garbage collection, keeping my C array a secret!  Shhh...
		sliceHeader.Cap = o_cap
		sliceHeader.Len = o_len
		sliceHeader.Data = o_data

		// fmt.Printf("\t%s\n", bmp.BitString(100))

		if err != nil {
			panic("Unable to read page (" + err.Error() + ")")
		}

	}
	def.Loaded = true
	// Test the first few values
	// fmt.Printf("Read Page (%d): ", depth)
	// for i := 0; i < 10; i++ {
	// 	fmt.Printf("%d, ", page.Access(i))
	// }
	// fmt.Printf("\n")

	return page
}

func SlicePointer(data []uint64) uintptr {

	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&data)))

	return sliceHeader.Data

}

func (self *TnyPage) BitString(length int) string {
	output := "Page for " + self.Column.Name + "\n"

	depth := self.Depth()
	for d := 0; d < depth; d++ {
		bmp := self.BitmapAtDepth(d)
		output += bmp.BitString(length) + "\n"
	}
	return output

}

func (self *TnyPage) BitmapAtDepth(depth int) PageBitmap {

	var bmp PageBitmap

	bmp.AllOnes = false
	bmp.cptr = C.tny_page_depth_data(self.cptr, C.int(depth))

	return bmp
}

func (self *TnyPage) Append(key_index int) {
	if key_index == 0 {
		panic("tny_page inser key_index: 0... Reserved for NULL values\n")
	}
	C.tny_page_append(self.cptr, C.int(key_index))
	self.dirty = true

	// ctest := C.tny_page_append(TnyPage.cptr, C.int(key_index))
	// test := int(ctest)
	// atest := test
	// if test < 0 {
	// 	atest = -test
	// }
	// if atest != key_index {

	// 	fmt.Printf("Error, TnyPage append failed (len:%d): %d != %d\n", TnyPage.Length(), key_index, ctest)
	// }

}

func (self *TnyPage) Seek(value int) PageBitmap {
	var bmp PageBitmap

	bmp.cptr = C.tny_page_seek(self.cptr, C.int(value))

	// fmt.Printf("Seek %s: %s\n", self.Column.Name, bmp.BitString(100))

	return bmp
}
func (self *TnyPage) SeekAnd(value int, bmp PageBitmap) PageBitmap {
	bmp.cptr = C.tny_page_seek_and(self.cptr, C.int(value), bmp.cptr)

	// fmt.Printf("SeekAnd %s: %s\n", self.Column.Name, bmp.BitString(100))
	return bmp
}
func (self *TnyPage) SeekOr(value int, bmp PageBitmap) PageBitmap {
	bmp.cptr = C.tny_page_seek_or(self.cptr, C.int(value), bmp.cptr)

	return bmp
}

func (self *TnyPage) Distinct(bitmap PageBitmap) Bitmap {
	//u64* tny_page_distinct(tny_page TnyPage, u64 *bitmap, int *bit_length) 

	// C_depth := C.tny_page_depth(self.cptr)

	// fmt.Printf("==================\nDistinct '%s' (depth: %d)\n==================\n", self.Column.Name, int(C_depth))
	// fmt.Printf("Page (%p):\n", self.cptr)
	// for i := 0; i < int(C_depth); i++ {
	// 	fmt.Printf("\t%s\n", self.BitmapAtDepth(i).BitString(100))
	// }
	// fmt.Printf("\nBitmap: (%s):\n", bitmap.BitString(100))

	length := C.int(0)
	var bmp Bitmap
	bmp.cptr = C.tny_page_distinct(self.cptr, bitmap.cptr, &length)
	bmp.BitLength = int(length)

	// fmt.Printf("Result: %s\n", bmp.BitString())

	return bmp

}

func (self *TnyPage) DistinctValues(bitmap Bitmap) TnyPageValueList {
	var pvl TnyPageValueList

	// fmt.Printf("bitmap.BitLength: %d\n", bitmap.BitLength)
	length := C.int(0)
	pvl.cptr = C.tny_bitmap_positions(bitmap.cptr, C.int(bitmap.BitLength), &length)

	var theGoSlice []int
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&theGoSlice)))
	sliceHeader.Cap = int(length)
	sliceHeader.Len = int(length)
	sliceHeader.Data = uintptr(unsafe.Pointer(pvl.cptr))

	// now theGoSlice is a normal Go slice backed by the C array
	pvl.Values = theGoSlice

	// fmt.Printf("DistinctValues(): {length: %d}\n", int(length))
	return pvl
}

func (self *TnyPage) Select(bitmap PageBitmap) TnyPageValueList {
	var pvl TnyPageValueList

	length := C.int(0)
	pvl.cptr = C.tny_page_select(self.cptr, bitmap.cptr, &length)

	var theGoSlice []int
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&theGoSlice)))
	sliceHeader.Cap = int(length)
	sliceHeader.Len = int(length)
	sliceHeader.Data = uintptr(unsafe.Pointer(pvl.cptr))

	// now theGoSlice is a normal Go slice backed by the C array
	pvl.Values = theGoSlice

	// fmt.Printf("Select %s, length: %d, first: %d\n", self.Column.Name, length, pvl.Values[0])
	// fmt.Printf("\t%s\n", bitmap.BitString(100))
	// fmt.Printf("\tPage {depth: %d, length: %d} \n", int(self.cptr.depth), int(self.cptr.length))

	return pvl
}

func (self *TnyPage) Access(index int) int {
	if self == nil {
		panic("TnyPage.Access: self == nil")
	}

	value := C.tny_page_access(self.cptr, C.int(index))
	return int(value)
}
