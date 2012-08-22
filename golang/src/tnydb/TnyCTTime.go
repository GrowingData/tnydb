package tnydb

import "bufio"
import "fmt"

// import "encoding/json"
import "encoding/binary"

// import "fmt"
import "time"

var place_holder_to_stop_fmt_error_TnyCTTime = fmt.Sprintf("keep 'fmt' import during debugging")

type TnyCTTime struct {
	Keys   []time.Time
	KeyMap map[int64]int
}

func (col *TnyCTTime) KeyCount() int {
	return len(col.Keys)
}

func NewTnyCTTime() *TnyCTTime {
	t := new(TnyCTTime)
	t.KeyMap = make(map[int64]int)

	// First key is always NULL
	var null time.Time
	t.Keys = append(t.Keys, null)

	return t
}

func (col *TnyCTTime) FindKey(value ValueContainer) (int, bool) {

	// fmt.Printf("FindKey() Looking for: %s, keys: %d\n", value.ToString(), len(col.KeyMap))

	index, found := col.KeyMap[value.VTime.UnixNano()]
	return index, found

}

func (col *TnyCTTime) InsertKey(value ValueContainer) int {
	new_index := len(col.Keys)

	col.KeyMap[value.VTime.UnixNano()] = len(col.Keys)
	col.Keys = append(col.Keys, value.VTime)

	return new_index
}

func (col *TnyCTTime) TypeLabel() string {
	return "Time"
}
func (col *TnyCTTime) ValueType() ValueType {
	return CT_TIME
}

func (col *TnyCTTime) KeyAt(idx int) ValueContainer {
	var vc ValueContainer
	vc.Type = CT_TIME
	vc.VTime = col.Keys[idx]
	return vc
}

//////////////////////////////////////////////////
//				SERIALIZATION					//
//////////////////////////////////////////////////

// Use a JSON encoding because I am not sure how to encode all the time
// information
func (self *TnyCTTime) Write(writer *bufio.Writer) {

	if err := binary.Write(writer, binary.LittleEndian, uint64(len(self.Keys)-1)); err != nil {
		panic("Unable to write Keys, failed writing length (" + err.Error() + ")")
	}
	for i := 1; i < len(self.Keys); i++ {
		//unix := t.Unix()
		nano := self.Keys[i].UnixNano()

		if err := binary.Write(writer, binary.LittleEndian, nano); err != nil {
			panic("TnyCTTime.Write: failed writing t.Unix(): " + err.Error())
		}
		// if err := binary.Write(writer, binary.LittleEndian, nano); err != nil {
		// 	panic("TnyCTTime.Write: failed writing t.UnixNano(): " + err.Error())
		// }
	}

}
func (self *TnyCTTime) Read(reader *bufio.Reader) {
	var keyCount uint64
	if err := binary.Read(reader, binary.LittleEndian, &keyCount); err != nil {
		panic("Unable to read Keys, failed reading length (" + err.Error() + ")")
	}

	//var unix int64
	var nano int64

	for i := uint64(0); i < keyCount; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &nano); err != nil {
			panic("TnyCTTime.Read: failed reading t.Unix(): " + err.Error())
		}
		// if err := binary.Read(reader, binary.LittleEndian, &nano); err != nil {
		// 	panic("TnyCTTime.Read: failed reading t.UnixNano(): " + err.Error())
		// }
		t := time.Unix(0, nano)
		self.KeyMap[t.UnixNano()] = len(self.Keys)
		self.Keys = append(self.Keys, t)

	}

}
