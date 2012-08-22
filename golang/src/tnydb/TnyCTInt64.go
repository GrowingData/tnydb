package tnydb

import "bufio"
import "encoding/binary"
import "fmt"

type TnyCTInt64 struct {
	Keys   []int64
	KeyMap map[int64]int
}

func NewTnyCTInt64() *TnyCTInt64 {
	t := new(TnyCTInt64)
	t.KeyMap = make(map[int64]int)

	// First key is always NULL
	t.Keys = append(t.Keys, 0)

	return t
}
func (col *TnyCTInt64) KeyCount() int {
	return len(col.Keys)
}

// Returns the index of the item within the Key map, along with a flag
// indicating if this Key exists
func (col *TnyCTInt64) FindKey(value ValueContainer) (int, bool) {
	index, found := col.KeyMap[value.VInt64]
	return index, found
}

func (col *TnyCTInt64) InsertKey(value ValueContainer) int {
	new_index := len(col.Keys)

	col.KeyMap[value.VInt64] = len(col.Keys)
	col.Keys = append(col.Keys, value.VInt64)

	return new_index
}

func (col *TnyCTInt64) TypeLabel() string {
	return "Integer"
}
func (col *TnyCTInt64) ValueType() ValueType {
	return CT_INTEGER
}

func (col *TnyCTInt64) KeyAt(idx int) ValueContainer {
	// fmt.Printf("TnyCTInt64.KeyAt: %d\n", idx)
	var vc ValueContainer
	vc.Type = CT_INTEGER
	vc.VInt64 = col.Keys[idx]
	return vc
}

//////////////////////////////////////////////////
//				Aggregators						//
//////////////////////////////////////////////////

func Int64_Sum(ct TnyColumnType) Aggregate {
	var total int64 = 0

	var agg Aggregate
	agg.Accumulate = func(page *TnyPage, bmp PageBitmap) {
		list := page.Select(bmp)
		for _, idx := range list.Values {
			// val := ct.KeyAt(idx).VInt64
			total += ct.KeyAt(idx).VInt64

		}
	}
	agg.AccumulateMerge = func(vc ValueContainer) {
		total += vc.VInt64
	}
	agg.Merge = func(other Aggregate) {
		// fmt.Printf("Int64_Sum.Merge\n")
		other.AccumulateMerge(VCInt64(total))
		// fmt.Printf("Int64_Sum.AccumulateMerge\n")
	}
	agg.Result = func() ValueContainer {
		return VCInt64(total)
	}

	return agg
}

//////////////////////////////////////////////////
//				SERIALIZATION					//
//////////////////////////////////////////////////

func (self *TnyCTInt64) Write(writer *bufio.Writer) {

	if err := binary.Write(writer, binary.LittleEndian, uint64(len(self.Keys)-1)); err != nil {
		panic("Unable to write Keys, failed writing length (" + err.Error() + ")")
	}

	// Dont output "NULL HOLDER" as it will be added for us
	for i := 1; i < len(self.Keys); i++ {
		if err := binary.Write(writer, binary.LittleEndian, self.Keys[i]); err != nil {
			panic("Unable to write Keys (" + err.Error() + ")")
		}
	}
}
func (self *TnyCTInt64) Read(reader *bufio.Reader) {
	var keyCount uint64
	if err := binary.Read(reader, binary.LittleEndian, &keyCount); err != nil {
		panic("Unable to read Keys, failed reading length (" + err.Error() + ")")

	}

	var val int64
	for i := uint64(0); i < keyCount; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
			panic("Unable to read Keys, failed reading key at " + fmt.Sprintf("%d", i) + " (" + err.Error() + ")")
		} else {
			self.KeyMap[val] = len(self.Keys)
			self.Keys = append(self.Keys, val)
		}
	}
}
