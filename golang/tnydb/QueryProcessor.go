package tnydb

import "fmt"

type RowMap map[uint64]*AggregateRow

var place_holder_to_stop_fmt_error_ProcessQuery = fmt.Sprintf("keep 'fmt' import during debugging")

func ProcessQuery(query *Query, db *TnyDatabase) *TnyTable {

	// Get my page count, will be the same for each TnyColumn

	aggregates, keys := SplitAggregatesKeys(query)

	var tbl *TnyTable
	if len(aggregates) > 0 {
		// Handling aggregates requires quite a bit of processing
		// compared to a normal query
		result := ProcessAggregateQuery(query, aggregates, keys)
		tbl = GetTable(result, query, db)
	} else {

		tbl = ProcessSelectQuery(query, db)

	}

	tbl.Print(-1)
	return tbl
}

func ProcessSelectQuery(query *Query, db *TnyDatabase) *TnyTable {
	////////////////////////////////////////////////////////
	//// 	Lets try it using a GO routine for each proc: 2542ms (2542)
	////////////////////////////////////////////////////////
	// NCPU := runtime.NumCPU()
	// cpus := make(chan int, NCPU)

	tbl := db.NewTable("")

	// Add colummns to the table
	for c := 0; c < len(query.Select); c++ {
		colName := query.Select[c].ColumnName
		colType := query.Select[c].Column.Type.ValueType()
		tbl.NewColumn(colName, colType)
	}

	// This can't be done in parrallel because we are relying
	// on values for columns being added in the correct order
	// to ensure that we don't get issues with keys being inserted
	// out of order

	// We could get some benefits from parallelism by doing the
	// WHERE and value loading into an intermediary data structure
	// on a per page basis, then copy that data into the final
	// table structure.  For systems with lots of processors that
	// might offer a benefit, but it would be minor on less than 4CPUs	

	// for c := 0; c < NCPU; c++ {
	// 	go func(c int) {
	offset := 0
	// fmt.Printf("CPU %d started... \n", c)
	page_count := query.Table.PageCount()
	for p := 0; p < page_count; p += 1 {
		bitmap, allZero := ProcessWhereForPage(query, p)
		if !allZero {
			for columnIndex, column := range query.Select {
				page := column.Column.Pages[p]
				keyIndexes := page.Select(bitmap)

				for _, ki := range keyIndexes.Values {
					tbl.Columns[columnIndex].Append(column.Column.Type.KeyAt(ki + offset))
				}
			}
		}

		offset += PAGE_MAX_VALUES
	}
	// cpus <- cpuResult
	// 	}(c)
	// }

	// result := <-cpus
	// for c := 1; c < NCPU; c++ {
	// 	<-cpus
	// }

	return tbl
}

// Determine what columns are aggregates and what are keys (essentially the GROUP BY 
// component of the query)
func SplitAggregatesKeys(query *Query) ([]*QueryExpression, []*QueryExpression) {
	aggregates := make([]*QueryExpression, 0)
	keys := make([]*QueryExpression, 0)

	for i := 0; i < len(query.Select); i++ {
		if len(query.Select[i].Function) == 0 {
			keys = append(keys, query.Select[i])
		} else {
			aggregates = append(keys, query.Select[i])
		}
	}
	return aggregates, keys
}

// Returns a bitmap indicating which rows within the Page have matched the
// WHERE conditions within the query. (E.g a Bitmap of [110101] would indicate
// that rows 0, 1, 3 and 5 match the query).  If the key specified in the
// WHERE clause doesn't exist within the column's keys, then an additional flag
// is returned (zeroed) which tells us that the bitmap is all zeroes, so additional
// processing can be avoided. 
func ProcessWhereForPage(query *Query, page_idx int) (PageBitmap, bool) {
	if len(query.Where) == 0 {
		// fmt.Printf("ProcessWhereForPage(): No conditions, all match\n")
		return BitmapOfOnes(), false
	}

	// Look for the key 
	seek := make([]int, len(query.Where))
	for i := 0; i < len(query.Where); i++ {
		index, found := query.Where[i].Column.Type.FindKey(query.Where[i].Value)
		if found {
			seek[i] = index

		} else {
			// A single condition failing means none of the condtiions
			// will match (as everything is AND for the moment)

			fmt.Printf("ProcessWhereForPage(): No matches\n")
			return BitmapOfZeroes(), true
		}
	}

	zeroed := false
	bmp := BitmapOfOnes()
	for i := 0; i < len(query.Where); i++ {
		where := query.Where[i]
		page := where.Column.Pages[page_idx]
		bmp = page.SeekAnd(seek[i], bmp)

		// fmt.Printf("ProcessWhereForPage(): Seeking.\n")
	}

	// fmt.Printf("ProcessWhereForPage: {%s}\n", bmp.BitString(100))

	return bmp, zeroed
}
