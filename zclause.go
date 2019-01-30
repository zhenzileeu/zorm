package zorm

import "strings"

type zWhereCond struct {
	logical 	string
	column  	string
	operation 	string
	value 		interface{}
}

func (cond *zWhereCond) build() (query string, args []interface{}) {
	switch cond.operation {
	case "RAW":
		query = cond.column
		args = cond.value.([]interface{})
	case "WHERE":
		return cond.value.(*zWhere).build()
	case "IN":
		args = cond.value.([]interface{})
		if args==nil || len(args) == 0 {
			query = ""
		} else {
			query = cond.column + " IN (" + strings.Trim(strings.Repeat("?,", len(args)), ",") + ")"
		}
	case "LIKE":
		args = nil
		pattern := cond.value.(string)
		if strings.HasPrefix(pattern, "'") && strings.HasSuffix(pattern, "'") {
			query = cond.column + " LIKE " + cond.value.(string)
		} else {

			query = cond.column + " LIKE '" + strings.Trim(pattern, "'") + "'"
		}
	case "RANGE":
		query = cond.column + " BETWEEN ? AND ?"
		args = cond.value.([]interface{})
	default:
		query = cond.column + " " + cond.operation + " ?"
		args = []interface{}{cond.value}
	}

	return
}

// Where clause
type zWhere struct {
	cond 		[]zWhereCond
}

func (where *zWhere) Raw(query string, args ...interface{}) (*zWhere) {
	if query == "" {
		return where
	}

	if where.cond == nil {
		where.cond = make([]zWhereCond, 0)
	}

	where.cond = append(where.cond, zWhereCond{
		logical: "AND",
		column: query,
		operation: "RAW",
		value: args,
	})
	return where
}

func (where *zWhere) Like(column,pattern string) (*zWhere) {
	if column == "" {
		return where
	}

	if where.cond == nil {
		where.cond = make([]zWhereCond, 0)
	}

	where.cond = append(where.cond, zWhereCond{
		logical: "AND",
		column: column,
		operation: "LIKE",
		value: pattern,
	})
	return where
}

func (where *zWhere) In(column string, value ...interface{}) (*zWhere) {
	if column=="" || value==nil || len(value)==0 {
		return where
	}

	if where.cond == nil {
		where.cond = make([]zWhereCond, 0)
	}

	where.cond = append(where.cond, zWhereCond{
		logical: "AND",
		column: column,
		operation: "IN",
		value: value,
	})
	return where
}

func (where *zWhere) Between(column string, v1, v2 interface{}) (*zWhere) {
	if column=="" || v1==nil || v2==nil {
		return where
	}

	if where.cond == nil {
		where.cond = make([]zWhereCond, 0)
	}

	where.cond = append(where.cond, zWhereCond{
		logical: "AND",
		column: column,
		operation: "RANGE",
		value: []interface{}{v1, v2},
	})
	return where
}

func (where *zWhere) Where(column,operation string, value interface{}) (*zWhere) {
	if column == "" || value==nil {
		return where
	}

	if where.cond == nil {
		where.cond = make([]zWhereCond, 0)
	}

	where.cond = append(where.cond, zWhereCond{
		logical: "AND",
		column: column,
		operation: operation,
		value: value,
	})
	return where
}

func (where *zWhere) AndWhere(cond *zWhere) (*zWhere) {
	if cond == nil {
		return where
	}

	if where.cond == nil {
		where.cond = make([]zWhereCond, 0)
	}

	where.cond = append(where.cond, zWhereCond{
		logical: "AND",
		operation: "WHERE",
		value: cond,
	})
	return where
}

func (where *zWhere) OrWhere(cond *zWhere) (*zWhere) {
	if cond == nil {
		return where
	}

	if where.cond == nil {
		where.cond = make([]zWhereCond, 0)
	}

	where.cond = append(where.cond, zWhereCond{
		logical: "OR",
		operation: "WHERE",
		value: cond,
	})
	return where
}

func (where *zWhere) build() (query string, args []interface{}) {
	query = ""
	args = make([]interface{}, 0)
	for _,cond := range where.cond {
		cquery, cargs := cond.build()

		if query == "" {
			query = "(" + cquery + ")"
		} else {
			query = query + " " + cond.logical + " (" + cquery + ")"
		}

		if cargs != nil {
			args = append(args, cargs...)
		}
	}

	return
}

// order by clause
type zOrderBy struct {
	columns 	[]string
}

func (orderby *zOrderBy) OrderBy(column,sort string) (*zOrderBy) {
	if column == "" {
		return orderby
	}

	if orderby.columns == nil {
		orderby.columns = make([]string, 0)
	}

	switch strings.ToUpper(sort) {
	case "":
		fallthrough
	case "ASC":
		orderby.columns = append(orderby.columns, column + " ASC")
	case "DESC":
		orderby.columns = append(orderby.columns, column + " DESC")
	}
	return orderby
}

func (orderby *zOrderBy) build() (query string, args []interface{}) {
	if orderby.columns == nil {
		return "",nil
	}
	query = strings.Join(orderby.columns, ",")
	return query,nil
}

// Limit clause
type zLimit struct {
	rowCount 		int64
	iOffset 		int64
}

func (limit *zLimit) Limit(rowCount int64) (*zLimit) {
	limit.rowCount = rowCount
	return limit
}

func (limit *zLimit) Offset(offset int64) (*zLimit) {
	limit.iOffset = offset
	return limit
}

// LIMIT row_count OFFSET Offset
func (limit *zLimit) build() (query string, args []interface{}) {
	query = "? OFFSET ?"
	args = []interface{}{limit.rowCount, limit.iOffset}

	return query, args
}

// LIMIT row_count
func (limit *zLimit) build2() (query string, args []interface{}) {
	query = "?"
	args = []interface{}{limit.rowCount}

	return query, args
}

// group by clause
type zGroupBy struct {
	columns 		[]string
	havingCond 		*zWhere
}

func (groupby *zGroupBy) GroupBy(column ...string) (*zGroupBy) {
	if column==nil || len(column)==0 {
		return groupby
	}

	if groupby.columns == nil {
		groupby.columns = make([]string, 0)
	}

	groupby.columns = append(groupby.columns, column...)
	return groupby
}

func (groupby *zGroupBy) Having(where *zWhere) (*zGroupBy) {
	groupby.havingCond = where
	return groupby
}

func (groupby *zGroupBy) build() (query string, args []interface{}) {
	if groupby.columns == nil || len(groupby.columns) == 0 {
		return "",nil
	}

	query = strings.Join(groupby.columns, ",")

	if groupby.havingCond != nil {
		cquery,cargs := groupby.havingCond.build()
		query = query + " HAVING " + cquery
		args = cargs
	} else {
		args = nil
	}

	return query,args
}

// join clause
type zJoinCondition interface {
	// joinCondition returns the join condition sql query
	joinCondition() (query string, args []interface{})
}

//
type zJoinOn struct {
	where 		*zWhere
}

func (join *zJoinOn) joinCondition() (query string, args []interface{}) {
	if join.where != nil {
		query,args = join.where.build()
		if query != "" {
			query = " ON " + query
		}

		return query,args
	}

	return "",nil
}

//
type zJoinUsing struct {
	columns 	[]string
}

func (join *zJoinUsing) joinCondition() (query string, args []interface{}) {
	if join.columns != nil && len(join.columns)>0 {
		return " USING (" + strings.Join(join.columns, ",") + ")", nil
	}

	return "",nil
}