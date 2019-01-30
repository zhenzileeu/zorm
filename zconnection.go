package zorm

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type ZConnection struct {
	connection *sql.DB
	transaction *sql.Tx
}

// OpenConnection defines a DB connection used in orm model
func (connection *ZConnection) Open(con ZConnectionCfg) (*ZConnection, error)  {
	var err error
	connection.connection,err = sql.Open("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
			con.UserName, con.Password, con.Host, con.Port,con.DbName, con.TimeoutSec, con.ReadTimeoutSec, con.WriteTimeoutSec))
	if err != nil {
		return nil, err
	}

	connection.connection.SetMaxIdleConns(con.MaxIdle)
	connection.connection.SetMaxOpenConns(con.MaxOpen)

	return connection, nil
}

// Close closes the database and prevents new queries from starting.
func (connection *ZConnection) Close() (error) {
	if connection.connection == nil {
		return errors.New("no connection")
	}

	return connection.connection.Close()
}

// Begin starts a transaction. The default isolation level is dependent on
// the driver.
func (connection *ZConnection) Begin() (err error) {
	if connection.connection == nil {
		return errors.New("model has no db connection defined")
	}

	connection.transaction, err = connection.connection.Begin()
	return
}

func (connection *ZConnection) Rollback() (error) {
	if connection.transaction == nil {
		return errors.New("no transaction")
	}

	return connection.transaction.Rollback()
}

func (connection *ZConnection) Commit() (error) {
	if connection.transaction == nil {
		return errors.New("no transaction")
	}

	return connection.transaction.Commit()
}

// NewModel gives an z-model with current db connection
func (connection *ZConnection) NewModel(table ZTable, sqlLogger zSqlLogger) (*zModel) {
	var model = new(zModel)
	model.table = table
	model.sqlLogger = sqlLogger
	model.connect(connection.connection)

	return model
}

type ZConnectionCfg struct {
	UserName 		string		`json:"user_name"`
	Password 		string		`json:"password"`
	Host 			string		`json:"host"`
	Port 			string		`json:"port"`
	DbName 			string		`json:"db_name"`
	TimeoutSec 		int 		`json:"timeout"`
	ReadTimeoutSec 	int			`json:"read_timeout"`
	WriteTimeoutSec	int 		`json:"write_timeout"`
	MaxIdle 		int 		`json:"max_idle"`
	MaxOpen 		int 		`json:"max_open"`
}
