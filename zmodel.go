package zorm

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"reflect"
)

type AssignList map[string]interface{}

func (list AssignList) Bind(obj interface{}) (*AssignList) {
	var r = reflect.ValueOf(obj).Elem()
	var numFields = r.NumField()

	for i := 0; i < numFields; i++ {
		if fk,ok := r.Type().Field(i).Tag.Lookup("zcolumn"); ok {
			list.Assign(fk, r.Field(i).Interface())
		}
	}
	return &list
}

func (list AssignList) Assign(column string, value interface{}) (*AssignList) {
	list[column] = value
	return &list
}

func (list AssignList) Delete(column string) (*AssignList) {
	delete(list, column)
	return &list
}

type zModel struct {
	table 		ZTable
	query 		*zQueryBuilder
	connection 	*sql.DB

	sqlLogger 	zSqlLogger
}

func (model *zModel) connect(connection *sql.DB) {
	model.connection = connection
}

func (model *zModel) NewQuery() (*zQueryBuilder) {
	model.query = new(zQueryBuilder)
	return model.query
}

func (model *zModel) Get(column *ZColumnList) (*zRows, *zModelErr) {
	var syntax = new(zSelect)
	syntax.table = model.table

	if model.query != nil {
		syntax.groupby = model.query.groupBy
		syntax.orderby = model.query.orderBy
		syntax.limit = model.query.limit
		syntax.where = model.query.where
	} else {
		syntax.groupby = nil
		syntax.orderby = nil
		syntax.limit = nil
		syntax.where = nil
	}

	if model.table.SoftDelete() != nil {
		syntax.where = new(zWhere).Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value()).AndWhere(syntax.where)
	}

	if column == nil {
		column = model.table.Columns()
	}
	var rows = column.makeRows()
	query,args,err := syntax.query(rows.columns...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	sqlRows,qerr := model.queryRows(query, args...)
	if qerr != nil {
		return nil, qerr
	}

	if err := rows.fill(sqlRows); err!=nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return rows, nil
}

func (model *zModel) First(column *ZColumnList) (*zRow, *zModelErr) {
	var syntax = new(zSelect)
	syntax.table = model.table

	if model.query != nil {
		syntax.groupby = model.query.groupBy
		syntax.limit = model.query.limit
		syntax.where = model.query.where
	} else {
		syntax.groupby = nil
		syntax.limit = nil
		syntax.where = nil
	}

	if model.table.SoftDelete() != nil {
		syntax.where = new(zWhere).Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value()).AndWhere(syntax.where)
	}

	if column == nil {
		column = model.table.Columns()
	}
	var row = column.makeRow()
	query,args,err := syntax.query(row.columns...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	sqlRow,qerr := model.queryRow(query, args...)
	if qerr != nil {
		return nil, qerr
	}

	if err := row.fill(sqlRow); err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return row,nil
}

func (model *zModel) Find(id int64, column *ZColumnList) (*zRow, *zModelErr) {
	var syntax = new(zSelect)
	syntax.table = model.table
	syntax.where = new(zWhere).Where(model.table.PrimaryKey(), "=", id)
	if syntax.table.SoftDelete() != nil {
		syntax.where.Where(syntax.table.SoftDelete().Column(), "=", syntax.table.SoftDelete().Value())
	}
	syntax.limit = new(zLimit).Limit(1).Offset(0)

	if column == nil {
		column = model.table.Columns()
	}
	var row = column.makeRow()
	query,args,err := syntax.query(row.columns...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	sqlRow,qerr := model.queryRow(query, args...)
	if qerr != nil {
		return nil, qerr
	}

	if err := row.fill(sqlRow); err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return row, nil
}

func (model *zModel) FindMany(id []int64, column *ZColumnList) (*zRows, *zModelErr) {
	var syntax = new(zSelect)
	syntax.table = model.table

	primaryIds := make([]interface{}, len(id))
	for i,v := range id {
		primaryIds[i] = v
	}
	syntax.where = new(zWhere).In(model.table.PrimaryKey(), primaryIds...)
	if model.table.SoftDelete() != nil {
		syntax.where.Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value())
	}
	syntax.orderby = new(zOrderBy).OrderBy(model.table.PrimaryKey(), "ASC")
	syntax.limit = new(zLimit).Limit(int64(len(primaryIds))).Offset(0)

	if column == nil {
		column = model.table.Columns()
	}
	var rows = column.makeRows()
	query,args,err := syntax.query(rows.columns...)
	if err != nil {
		return nil,&zModelErr{query:query, args:args, err:err}
	}

	sqlRows,qerr := model.queryRows(query, args...)
	if qerr != nil {
		return nil, qerr
	}

	if err := rows.fill(sqlRows); err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return rows, nil
}

func (model *zModel) Insert(list *AssignList) (id int64, err *zModelErr) {
	var syntax = new(zInsert)
	syntax.table = model.table
	syntax.assigns = *list

	query,args,serr := syntax.query()
	if serr != nil {
		return 0, &zModelErr{query:query, args:args, err:serr}
	}

	result,err := model.exec(query, args...)
	if err != nil {
		return 0, err
	}

	id, serr = result.LastInsertId()
	if serr != nil {
		return id, &zModelErr{query:query, args:args, err:serr}
	}

	return id,nil
}

func (model *zModel) Update(list *AssignList) (rowsAffected int64, err *zModelErr) {
	var syntax = new(zUpdate)
	syntax.table = model.table
	syntax.assigns = *list

	if model.query != nil {
		syntax.where = model.query.where
		syntax.orderBy = model.query.orderBy
		syntax.limit = model.query.limit
	} else {
		syntax.where = nil
		syntax.orderBy = nil
		syntax.limit = nil
	}

	if model.table.SoftDelete() != nil {
		syntax.where = new(zWhere).Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value()).AndWhere(syntax.where)
	}

	if syntax.assigns != nil {
		if id, ok := syntax.assigns[model.table.PrimaryKey()]; ok {
			where := new(zWhere).Where(model.table.PrimaryKey(), "=", id)
			syntax.where = where.AndWhere(syntax.where)
		}
	}

	query,args,serr := syntax.query()
	if serr != nil {
		return 0, &zModelErr{query:query, args:args, err:serr}
	}

	result,err := model.exec(query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected,serr = result.RowsAffected()
	if serr != nil {
		return rowsAffected, &zModelErr{query:query, args:args, err:serr}
	}

	return rowsAffected, nil
}

func (model *zModel) Delete() (rowsAffected int64, err *zModelErr) {
	if model.table.SoftDelete() == nil {
		return model.ForceDelete()
	}

	var softDelete = model.table.SoftDelete()
	return model.Update(&AssignList{softDelete.Column(): softDelete.DeleteValue()})
}

func (model *zModel) ForceDelete() (rowsAffected int64, err *zModelErr) {
	var syntax = new(zDelete)
	syntax.table = model.table

	if model.query != nil {
		syntax.where = model.query.where
		syntax.orderBy = model.query.orderBy
		syntax.limit = model.query.limit
	} else {
		syntax.where = nil
		syntax.orderBy = nil
		syntax.limit = nil
	}

	query,args,serr := syntax.query()
	if serr != nil {
		return 0, &zModelErr{query:query, args:args, err:serr}
	}

	result,err := model.exec(query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected,serr = result.RowsAffected()
	if serr != nil {
		return rowsAffected, &zModelErr{query:query, args:args, err:serr}
	}

	return rowsAffected,err
}

func (model *zModel) Count() (total int64, err *zModelErr) {
	var syntax = new(zSelect)
	syntax.table = model.table
	if model.query != nil {
		syntax.where = model.query.where
	} else {
		syntax.where = nil
	}

	if model.table.SoftDelete() != nil {
		syntax.where = new(zWhere).Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value()).AndWhere(syntax.where)
	}

	var row = ZColumnList{"count(1) as total": int64(0)}.makeRow()
	query,args,serr := syntax.query(row.columns...)
	if serr != nil {
		return 0, &zModelErr{query:query, args:args, err:serr}
	}

	sqlRow,err := model.queryRow(query, args...)
	if err != nil {
		return 0, err
	}

	if serr = row.fill(sqlRow); serr != nil {
		return 0, &zModelErr{query:query, args:args, err:serr}
	}

	if v, ok := row.Get("total"); ok {
		return v.(int64), nil
	}

	return 0, &zModelErr{query:query, args:args, err:errors.New("result parse error")}
}

func (model *zModel) exec(query string, args ...interface{}) (sql.Result, *zModelErr) {
	defer model.logQuery(query, args...)

	result,err := RawExec(model.connection, query, args...)
	if err != nil {
		return result,&zModelErr{query:query, args:args, err:err}
	}

	return result,nil
}

func (model *zModel) queryRows(query string, args ...interface{}) (*sql.Rows, *zModelErr) {
	defer model.logQuery(query, args...)

	rows,err := RawQuery(model.connection, query, args...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err: err}
	}

	return rows,nil
}

func (model *zModel) queryRow(query string, args ...interface{}) (*sql.Row, *zModelErr) {
	defer model.logQuery(query, args...)

	row,err := RawQueryRow(model.connection, query, args...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return row,nil
}

func (model *zModel) logQuery(query string, args ...interface{}) {
	if model.sqlLogger != nil {
		model.sqlLogger.LogSQL(query, args...)
	}
}

type zModelErr struct {
	query 		string
	args 		[]interface{}
	err 		error
}

func (err *zModelErr) Error() string {
	return err.err.Error()
}

func (err *zModelErr) Query() string {
	return err.query
}

func (err *zModelErr) Args() []interface{} {
	return err.args
}

type zSqlLogger interface {
	// LogSQL logs the sql query and args
	LogSQL(query string, args ...interface{})
}