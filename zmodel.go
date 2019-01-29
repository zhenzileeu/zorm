package zorm

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type AssignList map[string]interface{}

func NewTableQuery(table ZTable, sqlLogger zSqlLogger) (*zModel) {
	var model = new(zModel)
	model.table = table
	model.sqlLogger = sqlLogger

	return model
}

type zModel struct {
	table 		ZTable
	where 		*zWhere
	orderBy 	*zOrderBy
	groupBy 	*zGroupBy
	limit 		*zLimit

	sqlLogger 	zSqlLogger
}

func (model *zModel) Where(where *zWhere) (*zModel) {
	model.where = where
	return model
}

func (model *zModel) OrderBy(orderBy *zOrderBy) (*zModel) {
	model.orderBy = orderBy
	return model
}

func (model *zModel) GroupBy(groupBy *zGroupBy) (*zModel) {
	model.groupBy = groupBy
	return model
}

func (model *zModel) Limit(limit *zLimit) (*zModel) {
	model.limit = limit
	return model
}

func (model *zModel) Get(column ZColumnList) (*zRows, error) {
	var syntax = new(zSelect)
	syntax.table = model.table
	syntax.groupby = model.groupBy
	syntax.orderby = model.orderBy
	syntax.limit = model.limit

	if model.table.SoftDelete() != nil {
		syntax.where = new(zWhere).Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value()).AndWhere(model.where)
	} else {
		syntax.where = model.where
	}

	var rows = column.makeRows()
	query,args,err := syntax.query(rows.columns...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	sqlRows,err := model.queryRows(query, args...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	if err := rows.fill(sqlRows); err!=nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return rows, nil
}

func (model *zModel) First(column ZColumnList) (*zRow, error) {
	var syntax = new(zSelect)
	syntax.table = model.table
	syntax.groupby = model.groupBy
	syntax.orderby = model.orderBy
	syntax.limit = model.limit

	if model.table.SoftDelete() != nil {
		syntax.where = new(zWhere).Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value()).AndWhere(model.where)
	} else {
		syntax.where = model.where
	}

	var row = column.makeRow()
	query,args,err := syntax.query(row.columns...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	sqlRow,err := model.queryRow(query, args...)
	if err != nil {
		return nil, err
	}

	if err := row.fill(sqlRow); err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return row,nil
}

func (model *zModel) Find(id int64, column ZColumnList) (*zRow, error) {
	var syntax = new(zSelect)
	syntax.table = model.table
	syntax.where = new(zWhere).Where(model.table.PrimaryKey(), "=", id)
	if syntax.table.SoftDelete() != nil {
		syntax.where.Where(syntax.table.SoftDelete().Column(), "=", syntax.table.SoftDelete().Value())
	}
	syntax.limit = new(zLimit).Limit(1).Offset(0)

	var row = column.makeRow()
	query,args,err := syntax.query(row.columns...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	sqlRow,err := model.queryRow(query, args...)
	if err != nil {
		return nil, err
	}

	if err := row.fill(sqlRow); err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return row, nil
}

func (model *zModel) FindMany(id []int64, column ZColumnList) (*zRows, error) {
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

	var rows = column.makeRows()
	query,args,err := syntax.query(rows.columns...)
	if err != nil {
		return nil,&zModelErr{query:query, args:args, err:err}
	}

	sqlRows,err := model.queryRows(query, args...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	if err := rows.fill(sqlRows); err != nil {
		return nil, &zModelErr{query:query, args:args, err:err}
	}

	return rows, nil
}

func (model *zModel) Insert(list AssignList) (id int64, err error) {
	var syntax = new(zInsert)
	syntax.table = model.table
	syntax.assigns = list

	query,args,err := syntax.query()
	if err != nil {
		return 0, &zModelErr{query:query, args:args, err:err}
	}

	result,err := model.exec(query, args...)
	if err != nil {
		return 0, err
	}

	id, err = result.LastInsertId()
	if err != nil {
		return id, &zModelErr{query:query, args:args, err:err}
	}

	return id,nil
}

func (model *zModel) Update(list AssignList) (rowsAffected int64, err error) {
	var syntax = new(zUpdate)
	syntax.table = model.table
	syntax.assigns = list
	syntax.where = model.where
	syntax.orderBy = model.orderBy
	syntax.limit = model.limit

	query,args,err := syntax.query()
	if err != nil {
		return 0, &zModelErr{query:query, args:args, err:err}
	}

	result,err := model.exec(query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected,err = result.RowsAffected()
	if err != nil {
		return rowsAffected, &zModelErr{query:query, args:args, err:err}
	}

	return rowsAffected, nil
}

func (model *zModel) Delete() (rowsAffected int64, err error) {
	if model.table.SoftDelete() == nil {
		return model.ForceDelete()
	}

	var softDelete = model.table.SoftDelete()
	return model.Update(AssignList{softDelete.Column(): softDelete.DeleteValue()})
}

func (model *zModel) ForceDelete() (rowsAffected int64, err error) {
	var syntax = new(zDelete)
	syntax.table = model.table
	syntax.where = model.where
	syntax.orderBy = model.orderBy
	syntax.limit = model.limit

	query,args,err := syntax.query()
	if err != nil {
		return 0, &zModelErr{query:query, args:args, err:err}
	}

	result,err := model.exec(query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected,err = result.RowsAffected()
	if err != nil {
		return rowsAffected, &zModelErr{query:query, args:args, err:err}
	}

	return rowsAffected,err
}

func (model *zModel) Count() (int64) {
	var syntax = new(zSelect)
	syntax.table = model.table
	if model.table.SoftDelete() != nil {
		syntax.where = new(zWhere).Where(model.table.SoftDelete().Column(), "=", model.table.SoftDelete().Value()).AndWhere(model.where)
	} else {
		syntax.where = model.where
	}

	var row = ZColumnList{"count(1) as total": int64(0)}.makeRow()
	query,args,err := syntax.query(row.columns...)
	if err != nil {
		return 0
	}

	sqlRow,err := model.queryRow(query, args...)
	if err != nil {
		return 0
	}

	if err := row.fill(sqlRow); err != nil {
		return 0
	}

	if v, ok := row.Get("total"); ok {
		return v.(int64)
	}

	return 0
}

func (model *zModel) exec(query string, args ...interface{}) (sql.Result, *zModelErr) {
	defer model.logQuery(query, args...)

	result,err := RawExec(query, args...)
	if err != nil {
		return result,&zModelErr{query:query, args:args, err:err}
	}

	return result,nil
}

func (model *zModel) queryRows(query string, args ...interface{}) (*sql.Rows, *zModelErr) {
	defer model.logQuery(query, args...)

	rows,err := RawQuery(query, args...)
	if err != nil {
		return nil, &zModelErr{query:query, args:args, err: err}
	}

	return rows,nil
}

func (model *zModel) queryRow(query string, args ...interface{}) (*sql.Row, *zModelErr) {
	defer model.logQuery(query, args...)

	row,err := RawQueryRow(query, args...)
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
	return err.Error()
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