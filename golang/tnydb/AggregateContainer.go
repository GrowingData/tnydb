package tnydb

import "fmt"

type FN_Accumulate func(TnyPage *TnyPage, bmp PageBitmap)
type FN_AccumulateMerge func(number ValueContainer)
type FN_Merge func(aggregator Aggregate)
type FN_Result func() ValueContainer

type Aggregate struct {
	Accumulate      FN_Accumulate
	AccumulateMerge FN_AccumulateMerge
	Merge           FN_Merge
	Result          FN_Result
}

type AggregateType int

const (
	AT_UNKNOWN AggregateType = iota
	AT_DEFAULT
	AT_DISTINCT
)

type AggregateContainer struct {
	Type     AggregateType
	Default  Aggregate
	Distinct DistinctAggregate
}

func NewAggregateContainer(ct TnyColumnType, agg string) (*AggregateContainer, error) {
	container := new(AggregateContainer)

	switch agg {
	case "COUNT":
		agg := Aggregate_Count()
		container.Type = AT_DEFAULT
		container.Default = agg
	case "DISTINCT":
		agg := Aggregate_DistinctCount()
		container.Type = AT_DISTINCT
		container.Distinct = agg
	case "SUM":
		switch ct.ValueType() {
		case CT_INTEGER:
			agg := Int64_Sum(ct)
			container.Type = AT_DEFAULT
			container.Default = agg
		case CT_FLOAT64:
			agg := Float64_Sum(ct)
			container.Type = AT_DEFAULT
			container.Default = agg
		default:
			return nil, fmt.Errorf("SUM is not defined for Columns of type%s\n", ct.TypeLabel())
		}
	}

	return container, nil
}

func (self *AggregateContainer) Accumulate(page *TnyPage, bmp PageBitmap) {
	switch self.Type {
	case AT_DEFAULT:
		self.Default.Accumulate(page, bmp)
	case AT_DISTINCT:
		self.Distinct.Accumulate(page, bmp)
	}
}

func (self *AggregateContainer) Merge(other *AggregateContainer) {
	switch self.Type {
	case AT_DEFAULT:
		self.Default.Merge(other.Default)

	case AT_DISTINCT:
		self.Distinct.Merge(other.Distinct)
	}
}
func (self *AggregateContainer) Result() ValueContainer {
	switch self.Type {
	case AT_DEFAULT:
		return self.Default.Result()

	case AT_DISTINCT:
		return self.Distinct.Result()
	}

	var nullResult ValueContainer
	nullResult.IsNull = true
	return nullResult

}

func Aggregate_Count() Aggregate {
	var total int64 = 0

	var agg Aggregate
	agg.Accumulate = func(TnyPage *TnyPage, bmp PageBitmap) {
		total += int64(bmp.PopCount())
	}
	agg.AccumulateMerge = func(vc ValueContainer) {
		total += vc.VInt64
	}
	agg.Merge = func(other Aggregate) {
		other.AccumulateMerge(VCInt64(total))
	}
	agg.Result = func() ValueContainer {
		return VCInt64(total)
	}
	return agg
}

type DistinctAggregate struct {
	Accumulate     FN_Accumulate
	CombineBitmaps func(bmp Bitmap)
	Merge          func(aggregator DistinctAggregate)
	Result         FN_Result
}

func Aggregate_DistinctCount() DistinctAggregate {
	var agg DistinctAggregate

	var final Bitmap
	var first = true
	agg.Accumulate = func(page *TnyPage, bmp PageBitmap) {
		if first {
			first = false
			final = page.Distinct(bmp)
		} else {
			tmp := page.Distinct(bmp)
			defer tmp.Free()
			final.And(tmp)
		}
	}
	agg.CombineBitmaps = func(other Bitmap) {
		final.And(other)
	}
	agg.Merge = func(other DistinctAggregate) {
		other.CombineBitmaps(final)
	}
	agg.Result = func() ValueContainer {
		popcount := final.PopCount()
		defer final.Free()

		return VCInt64(int64(popcount))
	}
	return agg
}
