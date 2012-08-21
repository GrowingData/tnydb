package tnydb

import "bufio"
import "encoding/binary"
import "fmt"

type TnyCTString struct {
	Keys   []string
	KeyMap map[string]int
}

func NewTnyCTString() *TnyCTString {
	t := new(TnyCTString)
	t.KeyMap = make(map[string]int)

	// First key is always NULL
	t.Keys = append(t.Keys, "NULL HOLDER")

	return t
}
func (col *TnyCTString) KeyCount() int {
	return len(col.Keys)
}

func (col *TnyCTString) FindKey(value ValueContainer) (int, bool) {
	index, found := col.KeyMap[value.VString]

	// if found {
	// 	fmt.Printf("FindKey: %s\n", value.VString)
	// }

	return index, found

}
func (col *TnyCTString) InsertKey(value ValueContainer) int {
	new_index := len(col.Keys)

	col.KeyMap[value.VString] = len(col.Keys)
	col.Keys = append(col.Keys, value.VString)

	// fmt.Printf("Inserted Key \"%s\" at %d\n", value.VString, new_index)

	return new_index
}

func (col *TnyCTString) TypeLabel() string {
	return "String"
}
func (col *TnyCTString) ValueType() ValueType {
	return CT_STRING
}

func (col *TnyCTString) KeyAt(idx int) ValueContainer {
	var vc ValueContainer
	vc.Type = CT_STRING
	vc.VString = col.Keys[idx]
	return vc
}

//////////////////////////////////////////////////
//				SERIALIZATION					//
//////////////////////////////////////////////////

func (self *TnyCTString) Write(writer *bufio.Writer) {

	if err := binary.Write(writer, binary.LittleEndian, uint64(len(self.Keys)-1)); err != nil {
		panic("Unable to write Keys, failed writing length (" + err.Error() + ")")
	}
	// var zero byte
	// zero = 0
	for i := 1; i < len(self.Keys); i++ {
		if _, err := writer.WriteString(self.Keys[i] + "\n"); err != nil {
			panic("Unable to NewTnyCTString.Write (" + err.Error() + ")")
		}
	}
}
func (self *TnyCTString) Read(reader *bufio.Reader) {
	var keyCount uint64
	if err := binary.Read(reader, binary.LittleEndian, &keyCount); err != nil {
		panic("Unable to read Keys, failed reading length (" + err.Error() + ")")

	}

	for i := uint64(0); i < keyCount; i++ {

		if line, err := reader.ReadString('\n'); err != nil {
			panic("Unable to read Keys, failed reading key at " + fmt.Sprintf("%d", i) + " (" + err.Error() + ")")
		} else {
			// Remove the delimiter
			value := line[0 : len(line)-1]

			// Add the value to our keys array and our map
			self.KeyMap[value] = len(self.Keys)
			self.Keys = append(self.Keys, value)
		}
	}
}
