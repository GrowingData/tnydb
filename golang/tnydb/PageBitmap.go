package tnydb

// #cgo CFLAGS: -std=gnu99 -msse4.1 -I../../c/core/
// #cgo LDFLAGS: -L../../Debug/ -ltnydb
// #include "tny_page.h"
// #include "tny.h"
import "C"
import "unsafe"
import "reflect"
import "strconv"

type PageBitmap struct {
	cptr      *_Ctype_u64
	AllOnes   bool
	AllZeroes bool
}

func (bmp PageBitmap) Free() {
	C.tny_bitmap_free(bmp.cptr)
}

func BitmapOfOnes() PageBitmap {
	var bmp PageBitmap

	bmp.AllOnes = true
	bmp.cptr = C.tny_bitmap_create_ones()

	return bmp
}
func BitmapOfZeroes() PageBitmap {
	var bmp PageBitmap
	bmp.AllZeroes = true
	bmp.cptr = C.tny_bitmap_create()

	return bmp
}

func (bitmap PageBitmap) Copy() PageBitmap {
	var bmp PageBitmap
	bmp.cptr = C.tny_bitmap_copy(bitmap.cptr)
	bmp.AllOnes = bitmap.AllOnes
	bmp.AllZeroes = bitmap.AllZeroes

	return bmp
}

func (from PageBitmap) Update(to PageBitmap) {
	C.tny_bitmap_update(from.cptr, to.cptr)
}

func (bitmap PageBitmap) PopCount() int {
	cint := C.tny_popcnt(bitmap.cptr)
	return int(cint)
}

func (self PageBitmap) And(other PageBitmap) {
	C.tny_bitmap_and(self.cptr, other.cptr)
}

func (self PageBitmap) Or(other PageBitmap) {
	C.tny_bitmap_or(self.cptr, other.cptr)
}

func (self PageBitmap) Slice() []uint64 {
	word_length := (PAGE_MAX_VALUES / 64)
	var theGoSlice []uint64
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&theGoSlice)))
	sliceHeader.Cap = int(word_length)
	sliceHeader.Len = int(word_length)
	sliceHeader.Data = uintptr(unsafe.Pointer(self.cptr))

	return theGoSlice
}

func (self PageBitmap) BitString(length int) string {
	word_len := (length / 64)

	if length%64 > 0 {
		word_len += 1
	}

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
