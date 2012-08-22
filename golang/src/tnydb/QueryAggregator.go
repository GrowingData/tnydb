package tnydb

import "runtime"
import "fmt"

var stop_fmt_error_ProcessAggregateQuery = fmt.Sprintf("keep 'fmt' import during debugging")

func ProcessAggregateQuery(query *Query, aggregates []*QueryExpression, keys []*QueryExpression) RowMap {
	////////////////////////////////////////////////////////
	//// 	Lets try it using a GO routine for each proc: 2542ms (2542)
	////////////////////////////////////////////////////////
	NCPU := runtime.NumCPU()
	cpus := make(chan RowMap, NCPU)

	page_count := query.Table.PageCount()
	for c := 0; c < NCPU && c < page_count; c++ {
		// go func(c int) {
		// fmt.Printf("CPU %d started... \n", c)

		cpuResult := ProcessPageAggregate(query, c, aggregates, keys)
		for p := c + 1; p < page_count; p += NCPU {
			// fmt.Printf("Page %d started... \n", p)
			pageData := ProcessPageAggregate(query, p, aggregates, keys)
			MergeAggregatePages(cpuResult, pageData, aggregates)

		}
		cpus <- cpuResult

		// }(c)
	}

	result := <-cpus
	for c := 1; c < NCPU && c < page_count; c++ {
		MergeAggregatePages(result, <-cpus, aggregates)
	}

	return result
}

func ProcessPageAggregate(query *Query, pageIndex int, aggregates []*QueryExpression, keys []*QueryExpression) RowMap {
	// fmt.Printf(" > ProcessPageAggregate: %d\n", pageIndex)
	bmp, zeroed := ProcessWhereForPage(query, pageIndex)
	if !zeroed {
		// Create a TnyTable of all the combinations of keys that make sense for this page
		results := GetGroupByKeysForPage(query, pageIndex, bmp, aggregates, keys)

		// fmt.Printf("ProcessSelectForPage(): Got %d key combinations\n", len(results))
		FillAggregatesForPage(query, pageIndex, bmp, aggregates, results)

		return results
	}
	return nil
	// <-chilloutChannel

}
func MergeAggregatePages(page_a RowMap, page_b RowMap, aggregates []*QueryExpression) {
	// fmt.Printf("MergeAggregatePages\n")
	for row_b_key, row_b := range page_b {
		// Look up the row?
		row_a, found := page_a[row_b_key]
		if found {
			for a := 0; a < len(aggregates); a++ {
				row_a.Aggregates[a].Merge(row_b.Aggregates[a])
			}
			row_b = nil
		} else {
			page_a[row_b_key] = row_b
		}
	}
	page_b = nil

}

type AggregateRow struct {
	HashKey    uint64
	KeyValues  []ValueContainer
	Aggregates []*AggregateContainer
	Bmp        PageBitmap
}

// We need to be able to identify each keyed row so that
// we can merge aggregates from different pages
const (
	offset32 = 2166136261
	offset64 = 14695981039346656037
	prime32  = 16777619
	prime64  = 1099511628211
)

func NewHashKey(value int) uint64 {
	return UpdateHashKey(offset64, value)
}

func UpdateHashKey(key uint64, x int) uint64 {
	hashKey := key
	for x > 0 {
		b := byte(x) | 0x80
		hashKey *= prime64
		hashKey ^= uint64(b)

		x >>= 7
	}

	return hashKey

}

func NewAggregateRow(keys int, aggregates int) *AggregateRow {
	row := new(AggregateRow)
	row.KeyValues = make([]ValueContainer, keys)
	row.Aggregates = make([]*AggregateContainer, aggregates)
	return row
}

// Given a query that has a GROUP BY clause (that is aggregates which are calculated
// over each of the values for columns specified in the GROUP BY), our technique
// is to find all of the Keys for each of the GROUP BY columns that match the specified
// Bitmap (from ProcessWhereForPage).
// For multiple columns, we need to multiply the keys found for each column, so that if
// we have columns COL_A and COL_B with the values COL_A: [1, 2, 3] and COL_B: [10, 11], 
// we will end up with [ {1, 10}, {2, 10}, {3, 10}, {1, 11}, {2, 11}, {3, 11}].
// Of course, we don't actuall know that {3, 10} actuall exists as a row within the table
// so we need to actually lookup the combinations, which is where page.SeekAnd comes in to
// play, as it lets us only add additional keys that actually exist for the query.  So 
// that the addition of the next column can be done efficiently, we also store the Bitmap
// that corresponds to the Row Positions matching all the previous keys.
func GetGroupByKeysForPage(query *Query, page_idx int, bmp PageBitmap, aggregates []*QueryExpression, groupby []*QueryExpression) RowMap {
	results := make(RowMap, 0)

	// Firstly we build up a TnyTable of all the GROUP BY Columns for us to 
	// fill in with aggregates later
	page := groupby[0].Column.Pages[page_idx]
	ct := page.Column.Type

	// First run through.
	distinct_bmp := page.Distinct(bmp)
	defer distinct_bmp.Free()

	keys := page.DistinctValues(distinct_bmp)
	defer keys.Free()
	// bits := distinct_bmp.BitString()
	// fmt.Printf("GetGroupByKeysForPage: Rank: %d\n", bmp.PopCount())

	// fmt.Printf("Distinct keys:\n")
	for k := 0; k < len(keys.Values); k++ {
		row := NewAggregateRow(len(groupby), len(aggregates))

		v := ct.KeyAt(keys.Values[k])

		row.KeyValues[0] = v
		row.Bmp = page.SeekAnd(keys.Values[k], bmp.Copy())
		// row.Key = v.ToString()
		row.HashKey = NewHashKey(keys.Values[k])

		results[row.HashKey] = row

	}

	// None of this code below has been tested yet....
	for g := 1; g < len(groupby); g++ {

		page := groupby[g].Column.Pages[page_idx]
		tmp_results := make(RowMap, 0)
		for _, row := range results {
			// Next runs through.
			distinct_bmp := page.Distinct(row.Bmp)
			keys := page.DistinctValues(distinct_bmp)

			for k := 0; k < len(keys.Values); k++ {
				row := NewAggregateRow(len(groupby), len(aggregates))
				v := ct.KeyAt(page.Access(keys.Values[k]))
				row.KeyValues[g] = v

				row.HashKey = UpdateHashKey(row.HashKey, keys.Values[k])
				// row.Key = row.Key + "|" + v.ToString()
				row.Bmp = page.SeekAnd(keys.Values[k], row.Bmp.Copy())

				// Copy the previous key values
				for i := 0; i < g; i++ {
					row.KeyValues[i] = row.KeyValues[i]
				}

				tmp_results[row.HashKey] = row

				// Free the old bitmap
				row.Bmp.Free()
			}
			// fmt.Printf("GetGroupByKeysForPage(): {Column: %s, Values: %d}\n", groupby[0].ColumnName, len(keys.Values))

			distinct_bmp.Free()
			keys.Free()

		}
		results = tmp_results
	}

	return results
}

// Given a list of keys (results) with their bitmaps, read the page and accumate the
// aggregator.
func FillAggregatesForPage(query *Query, page_idx int, bmp PageBitmap, aggregates []*QueryExpression, results RowMap) {

	for _, row := range results {
		for a := 0; a < len(aggregates); a++ {
			ct := aggregates[a].Column.Type
			page := aggregates[a].Column.Pages[page_idx]

			agg, err := NewAggregateContainer(ct, aggregates[a].Function)

			if err != nil {
				panic(err)
			}

			row.Aggregates[a] = agg

			// fmt.Printf("Accumulate %s: %s\n", page.Column.Name, row.Bmp.BitString(100))
			row.Aggregates[a].Accumulate(page, row.Bmp)
		}

		// The bitmap for this row has now been used and is no longer required
		// as it is only used in the accumulate functions
		row.Bmp.Free()
	}
}

// Build up a table given our intermediary structure
func GetTable(rows RowMap, query *Query, db *TnyDatabase) *TnyTable {

	tbl := db.NewTable("")

	// Add colummns to the table
	for c := 0; c < len(query.Select); c++ {
		colName := query.Select[c].ColumnName
		colType := query.Select[c].Column.Type.ValueType()
		if len(query.Select[c].Function) != 0 {
			colName = query.Select[c].Function + "Of" + colName
		}

		tbl.NewColumn(colName, colType)
	}

	// Run through and add all the rows...
	for _, row := range rows {
		agg_pos := 0
		key_pos := 0
		for _, val := range query.Select {
			if len(val.Function) == 0 {
				tbl.Columns[agg_pos+key_pos].Append(row.KeyValues[key_pos])
				key_pos++
			} else {
				tbl.Columns[agg_pos+key_pos].Append(row.Aggregates[agg_pos].Result())
				agg_pos++
			}
		}
	}

	return tbl

}
