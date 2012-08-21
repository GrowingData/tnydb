package tnydb

import "strings"
import "strconv"
import "time"

// import "fmt"

type ValueType int

const (
	CT_UNKNOWN ValueType = iota
	CT_TIME
	CT_INTEGER
	CT_FLOAT64
	CT_STRING
)

type ValueContainer struct {
	VInt64      int64
	VTime       time.Time
	VString     string
	VFloat64    float64
	IsMaybeNull bool
	IsNull      bool
	Type        ValueType
}

func (vc ValueContainer) ToString() string {
	if vc.IsNull {
		return "NULL"
	}
	if vc.Type == CT_TIME {
		return vc.VTime.Format("2006-01-02") + " (time)"
	}

	if vc.Type == CT_INTEGER {
		return strconv.FormatInt(vc.VInt64, 10) + " (int)"
	}
	if vc.Type == CT_FLOAT64 {
		return strconv.FormatFloat(vc.VFloat64, 'f', 4, 64) + " (float)"
	}
	if vc.Type == CT_STRING {
		return string(vc.VString) + " (string)"
	}

	return "Unknown"
}

func (vc ValueContainer) Str() string {
	if vc.IsNull {
		return "NULL"
	}
	if vc.Type == CT_TIME {
		return vc.VTime.Format("2006-01-02")
	}

	if vc.Type == CT_INTEGER {
		return strconv.FormatInt(vc.VInt64, 10)
	}
	if vc.Type == CT_FLOAT64 {
		return strconv.FormatFloat(vc.VFloat64, 'f', 4, 64)
	}
	if vc.Type == CT_STRING {
		return "\"" + string(vc.VString) + "\""
	}

	return "Unknown"
}

func VCInt64(value int64) ValueContainer {
	var vc ValueContainer
	vc.VInt64 = value
	vc.Type = CT_INTEGER
	return vc
}
func VCFloat64(value float64) ValueContainer {
	var vc ValueContainer
	vc.VFloat64 = value
	vc.Type = CT_FLOAT64
	return vc
}
func VCString(value string) ValueContainer {
	var vc ValueContainer
	vc.VString = value
	vc.Type = CT_STRING
	return vc
}
func VCTime(value time.Time) ValueContainer {
	var vc ValueContainer
	vc.VTime = value
	vc.Type = CT_TIME
	return vc
}

func ParseString(value string) (ValueContainer, ValueType) {
	var vc ValueContainer
	var err error

	vc.IsMaybeNull = IsMaybeNull(value)

	value = strings.TrimSpace(value)

	// time_format := "2006-01-02 15:04:05 MST"
	if len(value) == 10 {
		parts := strings.Split(value, "-")
		if len(parts) == 3 {
			year, err := strconv.ParseInt(parts[0], 10, 32)
			if err == nil {
				month, err := strconv.ParseInt(parts[1], 10, 32)
				if err == nil {
					day, err := strconv.ParseInt(parts[2], 10, 32)

					if err == nil {
						// fmt.Printf("%d-%d-%d", year, month, day)
						vc.VString = value
						vc.VTime = time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
						vc.Type = CT_TIME
						return vc, CT_TIME
					}
				}
			}

		}
	}
	// This is way too slow man!
	// time_format := "2006-01-02"
	// time_val, err := time.Parse(time_format, value)
	// if err == nil {
	// 	vc.VString = value
	// 	vc.VTime = time_val
	// 	vc.Type = CT_TIME
	// 	return vc, CT_TIME
	// }

	// Try an integer yeah?
	int_val, err := strconv.ParseInt(value, 10, 64)

	if err == nil {
		// If you are an Int, then you can still work as a Float
		vc.VString = value
		vc.VFloat64 = float64(int(int_val))
		vc.VInt64 = int64(int_val)
		vc.Type = CT_INTEGER
		return vc, CT_INTEGER
	} else {
		float_val, err := strconv.ParseFloat(value, 64)
		if err == nil {
			vc.VString = value
			vc.VFloat64 = float_val
			vc.Type = CT_FLOAT64
			return vc, CT_FLOAT64
		} else {
			// Default to a string yeah?
			vc.VString = value
			vc.Type = CT_STRING
			return vc, CT_STRING

		}
	}

	return vc, CT_UNKNOWN

}

// Returns true if a value looks like a NULL
func IsMaybeNull(value string) bool {
	if value == "" {
		return true
	}

	lower := strings.ToLower(value)

	if lower == "n/a" || lower == "null" || lower == "na" || lower == "not available" {
		return true
	}

	return false
}
