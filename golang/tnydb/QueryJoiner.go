package tnydb

// import "fmt"

func ProcessJoin(db *TnyDatabase, a *TnyTable, b *TnyTable, aCol *TnyColumn, bCol *TnyColumn) *TnyTable {

	tbl := db.NewTable("")

	for _, col := range a.Columns {
		tbl.NewColumn(col.Name, col.Type.ValueType())
	}
	for _, col := range b.Columns {
		tbl.NewColumn(col.Name, col.Type.ValueType())
	}
	bColOffset := len(a.Columns)

	aKeys := aCol.Type.KeyCount()
	for i := 0; i < aKeys; i++ {
		key := aCol.Type.KeyAt(i)
		aKeyIndex := i

		// Check to see if this Key even exists in the
		// other table, as if it doesn't then we can avoid
		// a lot of looping!
		bKeyIndex, found := bCol.Type.FindKey(key)
		if found {
			aPageOffset := 0
			for aPageIndex, aPage := range aCol.Pages {
				aBmp := aPage.Seek(aKeyIndex)
				// Track how many matches there are for this key in
				// this page within table A
				aBmpPop := aBmp.PopCount()

				bPageOffset := 0
				for bPageIndex, bPage := range bCol.Pages {
					bBmp := bPage.Seek(bKeyIndex)
					// Track how many matches there are for this key in
					// this page within table B
					bBmpPop := bBmp.PopCount()

					// Grab the matching values for each column, do this before the
					// next loop to prevent calling .Select unnecessarily
					aPageValues := make([][]int, len(a.Columns))
					for ai, aValueColumn := range a.Columns {
						aPageValues[ai] = aValueColumn.Pages[aPageIndex].Select(aBmp).Values
					}

					bPageValues := make([][]int, len(b.Columns))
					for bi, bValueColumn := range b.Columns {
						bPageValues[bi] = bValueColumn.Pages[bPageIndex].Select(bBmp).Values
					}

					// This is where we multiply out the matches
					for aIdx := 0; aIdx < aBmpPop; aIdx++ {
						for bIdx := 0; bIdx < bBmpPop; bIdx++ {

							// This is where we fill the columns from table A 
							for i, aValueColumn := range a.Columns {
								val := aValueColumn.Type.KeyAt(aPageValues[i][aIdx] + aPageOffset)
								tbl.Columns[i].Append(val)
							}

							// This is where we fill the columns from table B
							for i, bValueColumn := range b.Columns {
								val := bValueColumn.Type.KeyAt(bPageValues[i][bIdx] + bPageOffset)
								tbl.Columns[i+bColOffset].Append(val)
							}

						}
					}
					bBmp.Free()

				}
				aBmp.Free()
				bPageOffset += PAGE_MAX_VALUES
			}
			aPageOffset += PAGE_MAX_VALUES

		}

	}

	return tbl

}
