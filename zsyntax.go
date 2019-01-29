package zorm

import (
	"github.com/pkg/errors"
	"strings"
)

type SyntaxError struct {
	syntax 		string
	err 		error
}

func (err *SyntaxError) Error() string {
	return err.syntax + " got something wrong, message: " + err.err.Error()
}

func (err *SyntaxError) Syntax() string {
	return err.syntax
}

// single table delete syntax
type zDelete struct {
	table 		ZTable
	where 		*zWhere
	orderBy 	*zOrderBy
	limit 		*zLimit
}

func (delete *zDelete) syntax() string {
	return "DELETE"
}

func (delete *zDelete) query() (query string, args []interface{}, err error) {
	if delete.table == nil || delete.table.Table()=="" {
		return "",nil, &SyntaxError{syntax:delete.syntax(), err:errors.New("no table defined")}
	}

	switch delete.table.(type) {
	case *ZJoinTable:
		return "", nil, &SyntaxError{syntax:delete.syntax(), err:errors.New("join table is not supported")}
	}

	query = "DELETE FROM " + delete.table.Table()
	args = make([]interface{}, 0)
	if delete.where != nil {
		wquery,wargs := delete.where.build()
		if wquery != "" {
			query = query + " WHERE " + wquery

			if wargs != nil {
				args = wargs
			}
		}
	}

	if delete.orderBy != nil {
		oquery,oargs := delete.orderBy.build()
		if oquery != "" {
			query = query + " ORDER BY " + oquery
			if oargs != nil {
				args = append(args, oargs...)
			}
		}
	}

	if delete.limit != nil {
		lquery,largs := delete.limit.build()
		if lquery != "" {
			query = query + " LIMIT " + lquery
			if largs != nil {
				args = append(args, largs...)
			}
		}
	}

	return query, args,nil
}

// multiple table delete syntax
type zMDelete struct {
	table		*ZJoinTable
	where 		*zWhere
}

func (delete *zMDelete) syntax() string {
	return "MDELETE"
}

func (delete *zMDelete) query(tableDeleted ...string) (query string, args []interface{}, err error) {
	if delete.table == nil || delete.table.Table()=="" {
		return "",nil, &SyntaxError{syntax:delete.syntax(), err:errors.New("no table defined")}
	}

	if tableDeleted==nil || len(tableDeleted) == 0 {
		return "",nil, &SyntaxError{syntax:delete.syntax(), err:errors.New("no table deleted")}
	}

	query = "DELETE " + strings.Join(tableDeleted, ",") + " FROM " + delete.table.Table()
	if args = tableArgs(delete.table); args == nil {
		args = make([]interface{}, 0)
	}
	if delete.where != nil {
		wquery,wargs := delete.where.build()
		if wquery != "" {
			query = query + " WHERE " + wquery

			if wargs != nil {
				args = wargs
			}
		}
	}

	return query, args,nil
}

// insert syntax
type zInsert struct {
	table 			ZTable
	assigns 		AssignList
}

func (insert *zInsert) syntax() string {
	return "INSERT"
}

func (insert *zInsert) query() (query string, args []interface{}, err error) {
	if insert.table == nil || insert.table.Table()=="" {
		return "",nil, &SyntaxError{syntax:insert.syntax(), err:errors.New("no table defined")}
	}

	if insert.assigns == nil || len(insert.assigns) == 0 {
		return "", nil, &SyntaxError{syntax:insert.syntax(), err:errors.New("no assignment")}
	}

	switch insert.table.(type) {
	case *ZJoinTable:
		return "", nil, &SyntaxError{syntax:insert.syntax(), err:errors.New("join table is not supported")}
	}

	query = ""
	args = make([]interface{}, 0)
	for column,value := range insert.assigns {
		query = query + column + "=?,"
		args = append(args, value)
	}
	query = "INSERT INTO " + insert.table.Table() + " SET " + strings.TrimRight(query, ",")

	return query,args,nil
}

// update syntax
type zUpdate struct {
	table 			ZTable
	assigns 		AssignList
	where 			*zWhere
	orderBy 		*zOrderBy
	limit 			*zLimit
}

func (update *zUpdate) syntax() string {
	return "UPDATE"
}

func (update *zUpdate) query() (query string, args []interface{}, err error) {
	if update.table == nil || update.table.Table()=="" {
		return "",nil, &SyntaxError{syntax:update.syntax(), err:errors.New("no table defined")}
	}

	if update.assigns==nil || len(update.assigns) == 0 {
		return "", nil, &SyntaxError{syntax:update.syntax(), err:errors.New("no assigns")}
	}

	query = ""
	if args = tableArgs(update.table); args == nil {
		args = make([]interface{},0)
	}
	for column,value := range update.assigns {
		query = query + column + "=?,"
		args = append(args, value)
	}

	query = "UPDATE " + update.table.Table() + " SET " + strings.TrimRight(query, ",")

	if update.where != nil {
		wquery,wargs := update.where.build()
		if wquery != "" {
			query = query + " WHERE " + wquery
			if wargs != nil {
				args = append(args, wargs...)
			}
		}
	}

	if update.orderBy != nil {
		oquery,oargs := update.orderBy.build()

		if oquery != "" {
			query = query + " ORDER BY " + oquery
			if oargs != nil {
				args = append(args, oargs...)
			}
		}
	}

	if update.limit != nil {
		lquery,largs := update.limit.build2()
		if lquery != "" {
			query = query + " LIMIT " + lquery
			if largs != nil {
				args = append(args, largs...)
			}
		}
	}

	return query, args, nil
}

// select syntax
type zSelect struct {
	table 		ZTable
	where 		*zWhere
	groupby 	*zGroupBy
	orderby 	*zOrderBy
	limit 		*zLimit
}

func (sel *zSelect) syntax() string {
	return "SELECT"
}

func (sel *zSelect) query(column ...string) (query string, args []interface{}, err error) {
	if sel.table == nil || sel.table.Table() == "" {
		return "", nil, &SyntaxError{syntax:sel.syntax(), err:errors.New("no table defined")}
	}

	if column==nil || len(column)==0 {
		return "", nil, &SyntaxError{syntax:sel.syntax(), err:errors.New("no column selected")}
	}

	query = "SELECT " + strings.Join(column, ",") + " FROM " + sel.table.Table()
	if args = tableArgs(sel.table); args == nil {
		args = make([]interface{}, 0)
	}
	if sel.where != nil {
		wquery,wargs := sel.where.build()
		if wquery != "" {
			query = query + " WHERE " + wquery
			args = append(args, wargs...)
		}
	}

	if sel.groupby != nil {
		gquery,gargs := sel.groupby.build()
		if gquery != "" {
			query = query + " GROUP BY " + gquery
			if gargs != nil {
				args = append(args, gargs...)
			}
		}
	}

	if sel.orderby != nil {
		oquery,oargs := sel.orderby.build()
		if oquery != "" {
			query = query + " ORDER BY " + oquery
			if oargs != nil {
				args = append(args, oargs...)
			}
		}
	}

	if sel.limit != nil {
		lquery,largs := sel.limit.build()
		if lquery != "" {
			query = query + " LIMIT " + lquery
			if largs != nil {
				args = append(args, largs...)
			}
		}
	}

	return query, args, nil
}

type zJoinSyntax 	string

func (syntax *zJoinSyntax) String() string {
	return string(*syntax)
}

const (
	zJoinUndefine 	zJoinSyntax  	=  ""
	zInnerJoin 		zJoinSyntax		=  "INNER JOIN"
	zLeftJoin 		zJoinSyntax		=  "LEFT JOIN"
	zRightJoin 		zJoinSyntax		=  "RIGHT JOIN"
)