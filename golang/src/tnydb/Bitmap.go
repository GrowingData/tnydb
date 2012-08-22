package tnydb

// #cgo CFLAGS: -std=gnu99 -msse4.1 -I../../../c/
// #cgo LDFLAGS: -L../../lib/ -ltnydb
// #include "tny_page.h"
// #include "tny.h"
import "C"
import "unsafe"
import "reflect"
import "strconv"

type Bitmap struct {
	cptr      *_Ctype_u64
	BitLength int
}

func (self Bitmap) And(other Bitmap) {
	C.tny_bitmap_and_longer(self.cptr, other.cptr, C.int(self.BitLength))
}

func (self Bitmap) Or(other Bitmap) {
	C.tny_bitmap_or_longer(self.cptr, other.cptr, C.int(self.BitLength))
}
func (self Bitmap) Free() {
	C.tny_bitmap_free(self.cptr)
}

func (self Bitmap) PopCount() int {
	popcount := C.tny_popcnt_longer(self.cptr, C.int(self.BitLength))
	return int(popcount)
}

func (self Bitmap) Slice() []uint64 {
	word_length := (self.BitLength / 64) + 1
	var theGoSlice []uint64
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&theGoSlice)))
	sliceHeader.Cap = (word_length)
	sliceHeader.Len = (word_length)
	sliceHeader.Data = uintptr(unsafe.Pointer(self.cptr))

	return theGoSlice
}

func (self Bitmap) BitString() string {
	length := self.BitLength
	word_len := (self.BitLength / 64) + 1
	var one uint64
	one = 1
	slice := self.Slice()
	rank := 0
	result := ""

	for w := 0; w < word_len; w++ {
		word := slice[w]

		for i := 0; i < 64; i++ {
			if (w*64)+i >= length {
				break
			}

			if word&(one<<uint(i)) != 0 {
				result += "1"
				rank++
			} else {
				result += "0"
			}
		}
	}

	return "Length: " + strconv.FormatInt(int64(length), 10) + " Rank: " + strconv.FormatInt(int64(rank), 10) + " {" + result + "}"

}
