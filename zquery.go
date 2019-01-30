package zorm

import (
	"database/sql"
	"github.com/pkg/errors"
	_ "github.com/go-sql-driver/mysql"
)

func WhereColumn(column, operation string, value interface{}) (*zWhere) {
	return new(zWhere).Where(column, operation, value)
}

func WhereBetween(column string, v1, v2 interface{}) (*zWhere) {
	return new(zWhere).Between(column, v1, v2)
}

func WhereIn(column string, value ...interface{}) (*zWhere) {
	return new(zWhere).In(column, value...)
}

func WhereRaw(query string, args ...interface{}) (*zWhere) {
	return new(zWhere).Raw(query, args...)
}

func WhereLike(column, pattern string) (*zWhere) {
	return new(zWhere).Like(column, pattern)
}

func JoinOn(where *zWhere) (*zJoinOn) {
	var joinOn = new(zJoinOn)
	joinOn.where = where
	return joinOn
}

func JoinUsing(column ...string) (*zJoinUsing) {
	var joinUsing = new(zJoinUsing)
	joinUsing.columns = column
	return joinUsing
}

func RawExec(query string, args ...interface{}) (sql.Result, error) {
	if connection == nil {
		return nil, errors.New("no db connection")
	}

	return connection.Exec(query, args...)
}

func RawQuery(query string, args ...interface{}) (*sql.Rows, error) {
	if connection == nil {
		return nil, errors.New("no db connection")
	}

	return connection.Query(query, args...)
}

func RawQueryRow(query string, args ...interface{}) (*sql.Row, error) {
	if connection == nil {
		return nil, errors.New("no db connection")
	}

	return connection.QueryRow(query, args...), nil
}

// zquery
type zQueryBuilder struct {
	where 		*zWhere
	orderBy 	*zOrderBy
	groupBy 	*zGroupBy
	limit 		*zLimit
}

func (query *zQueryBuilder) Where(column, operation string, value interface{}) (*zQueryBuilder) {
	if query.where == nil {
		query.where = new(zWhere).Where(column, operation, value)
	} else {
		query.where.Where(column, operation, value)
	}
	return query
}

func (query *zQueryBuilder) WhereRaw(rawQuery string, args ...interface{}) (*zQueryBuilder) {
	if query.where == nil {
		query.where = new(zWhere)
	}
	query.where.Raw(rawQuery, args...)
	return query
}

func (query *zQueryBuilder) WhereLike(column, pattern string) (*zQueryBuilder) {
	if query.where == nil {
		query.where = new(zWhere)
	}
	query.where.Like(column, pattern)
	return query
}

func (query *zQueryBuilder) WhereIn(column string, value ...interface{}) (*zQueryBuilder) {
	if query.where == nil {
		query.where = new(zWhere)
	}
	query.where.In(column, value...)
	return query
}

func (query *zQueryBuilder) WhereBetween(column string, v1, v2 interface{}) (*zQueryBuilder) {
	if query.where == nil {
		query.where = new(zWhere)
	}
	query.where.Between(column, v1, v2)
	return query
}

func (query *zQueryBuilder) WhereAnd(where *zWhere) (*zQueryBuilder) {
	if query.where == nil {
		query.where = where
	} else {
		query.where.AndWhere(where)
	}

	return query
}

func (query *zQueryBuilder) WhereOr(where *zWhere) (*zQueryBuilder) {
	if query.where == nil {
		query.where = where
	} else {
		query.where.OrWhere(where)
	}
	return query
}

func (query *zQueryBuilder) OrderBy(column, sort string) (*zQueryBuilder) {
	if query.orderBy == nil {
		query.orderBy = new(zOrderBy)
	}
	query.orderBy.OrderBy(column, sort)
	return query
}

func (query *zQueryBuilder) GroupBy(column ...string) (*zQueryBuilder) {
	if query.groupBy == nil {
		query.groupBy = new(zGroupBy)
	}
	query.groupBy.GroupBy(column...)
	return query
}

func (query *zQueryBuilder) Having(where *zWhere) (*zQueryBuilder) {
	if query.groupBy == nil {
		return query
	}

	query.groupBy.Having(where)
	return query
}

func (query *zQueryBuilder) Limit(rowCount int64) (*zQueryBuilder) {
	if query.limit == nil {
		query.limit = new(zLimit)
	}
	query.limit.Limit(rowCount)
	return query
}

func (query *zQueryBuilder) Paginate(offset,limit int64) (*zQueryBuilder) {
	if query.limit == nil {
		query.limit = new(zLimit)
	}
	query.limit.Limit(limit).Offset(offset)
	return query
}