package zorm

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var connection 	*sql.DB = nil

// OpenConnection defines a DB connection used in orm model
func Open(con ZConnectionCfg) (error)  {
	connection,err := sql.Open("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
			con.UserName, con.Password, con.Host, con.Port,con.DbName, con.TimeoutSec, con.ReadTimeoutSec, con.WriteTimeoutSec))
	if err != nil {
		return err
	}

	connection.SetMaxIdleConns(con.MaxIdle)
	connection.SetMaxOpenConns(con.MaxOpen)

	return nil
}

// Begin starts a transaction
func Begin() (*sql.Tx, error) {
	return connection.Begin()
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
