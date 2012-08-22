package tnydb

import "bufio"
import "strings"

type AggregateIntermediary struct {

}

type TnyColumnType interface {
	TypeLabel() string
	ValueType() ValueType
	FindKey(value ValueContainer) (int, bool)
	InsertKey(val ValueContainer) int
	KeyAt(at int) ValueContainer
	KeyCount() int

	Write(writer *bufio.Writer)
	Read(writer *bufio.Reader)
}

func ValueTypeFromName(name string) ValueType {
	name = strings.TrimSpace(name)
	switch name {
	case "Int64", "Integer", "Int":
		return CT_INTEGER
	case "String":
		return CT_STRING
	case "Time", "Date", "DateTime":
		return CT_TIME
	case "Float64", "Float", "Double":
		return CT_FLOAT64
	}

	panic("Unknown ValueType: " + name)

	return CT_UNKNOWN
}

func GetColumnTypeFromName(name string) TnyColumnType {
	v := ValueTypeFromName(name)
	return GetValueType(v)
}
func GetValueType(val ValueType) TnyColumnType {
	switch val {
	case CT_INTEGER:
		return NewTnyCTInt64()
	case CT_STRING:
		return NewTnyCTString()
	case CT_TIME:
		return NewTnyCTTime()
	case CT_FLOAT64:
		return NewTnyCTFloat64()
	}
	return nil
}
func TnyColumnTypeLabel(val ValueType) string {
	switch val {
	case CT_INTEGER:
		return "Integer"
	case CT_STRING:
		return "String"
	case CT_FLOAT64:
		return "Float64"
	case CT_TIME:
		return "Time"
	}

	return "Unknown"
}
