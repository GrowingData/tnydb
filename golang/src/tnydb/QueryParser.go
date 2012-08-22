package tnydb

import (
	"bufio"
	"io"
	// "os"
	// "strconv"
	// "strings"
	"fmt"
)

const (
	PHASE_FROM = byte(iota)
	PHASE_WHERE
	PHASE_SELECT
)

type QueryExpression struct {
	Function   string
	ColumnName string
	Column     *TnyColumn
}

type QueryFilter struct {
	ColumnName string
	Column     *TnyColumn
	Compare    string
	Value      ValueContainer
}

type Query struct {
	From   string
	Table  *TnyTable
	Select []*QueryExpression
	Where  []*QueryFilter
}

type Element struct {
	Type byte
	Val  interface{}
}

type QueryParser struct {
	in *bufio.Reader
}

func NewQueryParser(input io.Reader) (r *QueryParser) {
	r = new(QueryParser)
	r.in = bufio.NewReader(input)
	return r
}

func (q *QueryFilter) ToString() string {
	return q.Column.Name + "(" + TnyColumnTypeLabel(q.Column.Type.ValueType()) + ") " + q.Compare + " " + q.Value.ToString()
}
func (q *QueryExpression) ToString() string {
	return q.Function + "(" + q.ColumnName + ")"
}

func (q *Query) ToString() string {
	output := "SELECT \n"
	for i := 0; i < len(q.Select); i++ {
		output += "\t" + q.Select[i].ToString() + "\n"
	}
	output += "FROM " + q.From + "\n"
	output += "WHERE" + "\n"

	for i := 0; i < len(q.Where); i++ {
		output += "\t" + q.Where[i].ToString() + "\n"
	}
	return output
}

func (r *QueryParser) readToken() (t string, e error) {
	b := make([]byte, 1)

	cnt := 0
	for _, e = r.in.Read(b); e == nil; _, e = r.in.Read(b) {
		switch b[0] {
		case ' ', '\t', '\n', '\r':
			return
		case '[':
			r.in.UnreadByte()
			return
		case ')', ']', '|':
			r.in.UnreadByte()
			return
		case '"':
			if len(t) == 0 {
				t = "\""
				return
			} else {
				return
			}
		case '(':
			r.in.UnreadByte()
			return

		case '=', '<', '>', '+', '/', ',':
			if cnt == 0 {
				t = string(b)
				return
			} else {
				r.in.UnreadByte()
				return
			}
			return
		// case '"', '\'', '(':
		// 	return "", fmt.Errorf("Syntax error: Unexpected %s\n", b)
		default:
			t += string(b)
		}
		cnt++
	}
	return
}

func (r *QueryParser) ReadAll(db *TnyDatabase) (*Query, error) {
	phase := PHASE_FROM
	query := new(Query)
	filter := new(QueryFilter)
	expression := new(QueryExpression)

	b := make([]byte, 1)
	var e error
	for _, e = r.in.Read(b); e == nil; _, e = r.in.Read(b) {
		// fmt.Printf("> %s, %d\n", b, phase)

		// Whitespace... Keep on reading please!
		switch b[0] {
		case ' ', '\t', '\n', '\r':
			continue
		}

		switch phase {
		case PHASE_FROM:
			if len(query.From) == 0 {
				r.in.UnreadByte()
				query.From, _ = r.readToken()

				tbl, found := db.Tables[query.From]
				if found {
					query.Table = tbl
				} else {
					return nil, fmt.Errorf("The TnyTable \"%s\" is unknown in database \"%s\".", query.From, db.Name)
				}

				// fmt.Printf("query.From: %s\n", query.From)
			} else {
				if b[0] == '[' {
					phase = PHASE_WHERE
					// fmt.Printf("-> Phase: PHASE_WHERE [\n")
				} else {
					if b[0] == '|' {

						phase = PHASE_SELECT
						// fmt.Printf("-> Phase: PHASE_SELECT ('|')\n")
					}

				}
			}
			continue

		case PHASE_WHERE:
			if b[0] == ']' || b[0] == '|' {
				phase = PHASE_SELECT
				// fmt.Printf("-> Phase: PHASE_SELECT (']' or '|')\n")
				continue
			} else {
				r.in.UnreadByte()

				if len(filter.ColumnName) == 0 {
					filter.ColumnName, _ = r.readToken()

					// Try to find the TnyColumn
					col, found := query.Table.ColumnsMap[filter.ColumnName]
					if found {
						// fmt.Printf("-> TnyColumn: %s\n", col.Name)
						filter.Column = col
					} else {
						return nil, fmt.Errorf("The TnyColumn \"%s\" in conditional clause is unknown in TnyTable  \"%s\".", filter.ColumnName, query.Table.Name)

					}

				} else if len(filter.Compare) == 0 {
					filter.Compare, _ = r.readToken()
				} else { //if filter.Value == nil {
					token, _ := r.readToken()
					if token == "\"" {
						token, _ = r.readToken()
					}

					// fmt.Printf("-> Value: %s\n", token)

					value, ptype := ParseString(token)
					ctype := filter.Column.Type.ValueType()

					if ptype <= ctype {
						filter.Value = value

						query.Where = append(query.Where, filter)
						filter = new(QueryFilter)
					} else {
						return nil, fmt.Errorf("The value \"%s\" in conditional clause is not a valid type for \"%s\".", token, TnyColumnTypeLabel(ctype))

					}
				}

			}
		case PHASE_SELECT:
			switch b[0] {
			case '(':
				if len(expression.Function) > 0 {
					continue
				}
			case ')', ',', '|':
				continue
			}

			r.in.UnreadByte()
			token, _ := r.readToken()
			// fmt.Printf("-> SELECT TOKEN: %s\n", token)
			if token == "->" {
				continue

			} else {

				switch token {
				case "SUM", "AVG", "COUNT":
					{
						expression.Function = token
					}
				default:
					{
						expression.ColumnName = token

						// Try to find the TnyColumn
						col, found := query.Table.ColumnsMap[expression.ColumnName]
						if found {
							expression.Column = col
						} else {
							return nil, fmt.Errorf("The TnyColumn \"%s\" in the projection clause unknown in TnyTable  \"%s\".", expression.ColumnName, query.Table.Name)

						}

						query.Select = append(query.Select, expression)
						expression = new(QueryExpression)

					}

				}

			}

		}

	}
	// fmt.Printf("SELECT: %s\n", query.Select)
	// fmt.Printf("FROM: %s\n", query.From)
	// fmt.Printf("WHERE: %s\n", query.Where)

	return query, nil

}

func (r *QueryParser) ParseQuery() {
	// Just a little tester, yo...

	t, err := r.readToken()
	if err == nil {
		fmt.Printf("Token 1: %s\n", t)

	} else {
		fmt.Printf("Error: %s\n", err.Error())

	}

}
