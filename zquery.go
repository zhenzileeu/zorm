package zorm

import (
	"database/sql"
	"github.com/pkg/errors"
	_ "github.com/go-sql-driver/mysql"
)

var connection 	*sql.DB = nil

// OpenConnection defines a DB connection used in orm model
func OpenConnection(conn *sql.DB)  {
	connection = conn
}

// Begin starts a transaction
func Begin() (*sql.Tx, error) {
	return connection.Begin()
}

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

func OrderBy(column, sort string) (*zOrderBy) {
	return new(zOrderBy).OrderBy(column, sort)
}

func Limit(rowCount, offset int64) (*zLimit) {
	return new(zLimit).Limit(rowCount).Offset(offset)
}

func GroupBy(column ...string) (*zGroupBy) {
	return new(zGroupBy).GroupBy(column...)
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