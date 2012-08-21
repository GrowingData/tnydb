package tnydb

import "bufio"
import "encoding/binary"
import "fmt"

type TnyCTFloat64 struct {
	Keys   []float64
	KeyMap map[float64]int
}

func NewTnyCTFloat64() *TnyCTFloat64 {
	t := new(TnyCTFloat64)
	t.KeyMap = make(map[float64]int)

	// First key is always NULL
	t.Keys = append(t.Keys, 0)

	return t
}
func (col *TnyCTFloat64) KeyCount() int {
	return len(col.Keys)
}

func (col *TnyCTFloat64) FindKey(value ValueContainer) (int, bool) {
	index, found := col.KeyMap[value.VFloat64]
	return index, found

}

func (col *TnyCTFloat64) InsertKey(value ValueContainer) int {
	new_index := len(col.Keys)

	col.KeyMap[value.VFloat64] = len(col.Keys)
	col.Keys = append(col.Keys, value.VFloat64)

	return new_index
}

func (col *TnyCTFloat64) TypeLabel() string {
	return "Float64"
}
func (col *TnyCTFloat64) ValueType() ValueType {
	return CT_FLOAT64
}

func (col *TnyCTFloat64) KeyAt(idx int) ValueContainer {
	var vc ValueContainer
	vc.Type = CT_FLOAT64
	vc.VFloat64 = col.Keys[idx]
	return vc
}

//////////////////////////////////////////////////
//				AGGREGATOR						//
//////////////////////////////////////////////////

func Float64_Sum(ct TnyColumnType) Aggregate {
	var total float64 = 0

	var agg Aggregate
	agg.Accumulate = func(page *TnyPage, bmp PageBitmap) {
		list := page.Select(bmp)
		for idx := range list.Values {
			total += ct.KeyAt(idx).VFloat64
		}
	}
	agg.AccumulateMerge = func(vc ValueContainer) {
		total += vc.VFloat64
	}
	agg.Merge = func(other Aggregate) {
		other.AccumulateMerge(VCFloat64(total))
	}
	agg.Result = func() ValueContainer {
		return VCFloat64(total)
	}

	return agg
}

//////////////////////////////////////////////////
//				SERIALIZATION					//
//////////////////////////////////////////////////

func (self *TnyCTFloat64) Write(writer *bufio.Writer) {

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
func (self *TnyCTFloat64) Read(reader *bufio.Reader) {
	var keyCount uint64
	if err := binary.Read(reader, binary.LittleEndian, &keyCount); err != nil {
		panic("Unable to read Keys, failed reading length (" + err.Error() + ")")

	}

	var val float64
	for i := uint64(0); i < keyCount; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
			panic("Unable to read Keys, failed reading key at " + fmt.Sprintf("%d", i) + " (" + err.Error() + ")")
		} else {
			self.KeyMap[val] = len(self.Keys)
			self.Keys = append(self.Keys, val)
		}
	}

}
