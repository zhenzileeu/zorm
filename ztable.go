package zorm

import (
	"database/sql"
	"reflect"
	"time"
	"strings"
	"github.com/pkg/errors"
	_ "github.com/go-sql-driver/mysql"
)

type ZMap map[string]interface{}

type ZTable interface {
	// table name
	Table() (name string)

	// primary key
	PrimaryKey() string

	// column list
	Columns() (*ZColumnList)

	// soft delete
	SoftDelete() SoftDelete
}

type ZJoinTable struct {
	TableReference 		ZTable
	Alias 				string
	joinQuery 			string
	joinArgs 			[]interface{}
}

func (joinTable *ZJoinTable) Table() (name string) {
	if joinTable.TableReference != nil {
		var refTable = joinTable.TableReference.Table()
		if joinTable.Alias != "" {
			refTable = refTable + " AS " + joinTable.Alias
		}

		return refTable + " " + joinTable.joinQuery
	}
	return ""
}

func (joinTable *ZJoinTable) tableArgs() (args []interface{}) {
	if args = tableArgs(joinTable.TableReference); args != nil {
		if joinTable.joinArgs != nil {
			args = append(args, joinTable.joinArgs...)
		}

		return args
	}

	return joinTable.joinArgs
}

func (joinTable *ZJoinTable) PrimaryKey() string {
	return joinTable.TableReference.PrimaryKey()
}

func (joinTable *ZJoinTable) Columns() (*ZColumnList) {
	return joinTable.TableReference.Columns()
}

func (joinTable *ZJoinTable) SoftDelete() SoftDelete {
	return joinTable.TableReference.SoftDelete()
}

func (joinTable *ZJoinTable) Join(table ZTable, alias string, joinCondition zJoinCondition) (*ZJoinTable) {
	return joinTable.join(table, zInnerJoin, alias, joinCondition)
}

func (joinTable *ZJoinTable) InnerJoin(table ZTable, alias string, joinCondition zJoinCondition) (*ZJoinTable) {
	return joinTable.join(table, zInnerJoin, alias, joinCondition)
}

func (joinTable *ZJoinTable) LeftJoin(table ZTable, alias string, joinCondition zJoinCondition) (*ZJoinTable) {
	return joinTable.join(table, zLeftJoin, alias, joinCondition)
}

func (joinTable *ZJoinTable) RightJoin(table ZTable, alias string, joinCondition zJoinCondition) (*ZJoinTable) {
	return joinTable.join(table, zRightJoin, alias, joinCondition)
}

func (joinTable *ZJoinTable) join(table ZTable, joinSyntax zJoinSyntax, alias string, joinCondition zJoinCondition) (*ZJoinTable) {
	if table == nil || joinSyntax==zJoinUndefine {
		return joinTable
	}

	switch joinSyntax {
	case zInnerJoin,zLeftJoin,zRightJoin:
		name := table.Table()
		joinTable.joinQuery = joinTable.joinQuery + " " + joinSyntax.String() + " (" + name + ")"
	default:
		return joinTable
	}

	if args := tableArgs(table); args != nil {
		joinTable.joinArgs = append(joinTable.joinArgs, args...)
	}

	if alias != "" {
		joinTable.joinQuery = joinTable.joinQuery + " AS " + alias
	}

	if joinCondition != nil {
		query,args := joinCondition.joinCondition()
		if query != "" {
			joinTable.joinQuery = joinTable.joinQuery + " " + query

			if args != nil {
				joinTable.joinArgs = append(joinTable.joinArgs, args...)
			}
		}
	}

	return joinTable
}

func tableArgs(table ZTable) (args []interface{}) {
	switch table.(type) {
	case *ZJoinTable:
		return table.(*ZJoinTable).tableArgs()
	}

	return nil
}

type SoftDelete interface {
	// soft delete column
	Column() string

	// soft delete value
	// if one row takes this value
	// it is not deleted
	Value() interface{}

	// soft delete value
	// if one row takes this value
	// it is deleted
	DeleteValue() interface{}
}

type ZColumnList ZMap

func (column ZColumnList) Bind(obj interface{}) (*ZColumnList) {
	var r = reflect.ValueOf(obj).Elem()
	var numFields = r.NumField()

	for i := 0; i < numFields; i++ {
		if fk,ok := r.Type().Field(i).Tag.Lookup("column"); ok {
			column.Append(fk, reflect.Zero(r.Field(i).Type()).Interface())
		}
	}
	return &column
}

func (column ZColumnList) makeRow() (*zRow) {
	var row = new(zRow)
	row.columns = make([]string, 0)
	row.value = make([]interface{}, 0)
	for k,v := range column {
		row.columns = append(row.columns, k)
		row.value = append(row.value, v)
	}

	return row
}

func (column ZColumnList) makeRows() (*zRows) {
	var rows = new(zRows)
	rows.columns = make([]string, 0)
	rows.value = make([]interface{}, 0)
	for k,v := range column {
		rows.columns = append(rows.columns, k)
		rows.value = append(rows.value, v)
	}

	return rows
}

func (column ZColumnList) Append(key string, value interface{}) (*ZColumnList) {
	column[key] = value
	return &column
}

type zScanner interface {
	//
	Scan(dest ...interface{}) error
}

type zRows struct {
	columns 	[]string
	value 		[]interface{}
	rows 		[]zRow
}

func (rows *zRows) fill(sqlRows *sql.Rows) (error) {
	defer sqlRows.Close()

	rows.rows = make([]zRow, 0)
	for sqlRows.Next()  {
		row := zRow{columns:rows.columns, value:rows.value}
		err := row.fill(sqlRows)
		if err != nil {
			rows.rows = nil
			return err
		}
		rows.rows = append(rows.rows, row)
	}

	if err:=sqlRows.Err(); err != nil {
		rows.rows = nil
		return err
	}

	return nil
}

func (rows *zRows) Rows() ([]zRow) {
	return rows.rows
}

func (rows *zRows) Count() (int64) {
	if rows.rows == nil {
		return 0
	}

	return int64(len(rows.rows))
}

type zRow struct {
	columns 		[]string
	value 			[]interface{}
	filledMap 		ZMap
}

func (row *zRow) Bind(obj interface{}) (error) {
	r := reflect.ValueOf(obj).Elem()

	switch r.Kind() {
	case reflect.Struct:
		numFields := r.NumField()
		for i := 0; i < numFields; i++ {
			field := r.Type().Field(i)
			if column,ok := field.Tag.Lookup("column"); ok {
				if v, ok := row.filledMap[column]; ok {
					r.Field(i).Set(reflect.ValueOf(v))
				}
			}
		}
	default:
		return errors.New("you should bind an struct")
	}

	return nil
}

func (row *zRow) Get(key string) (interface{}, bool) {
	if row.filledMap == nil || len(row.filledMap) == 0 {
		return nil,false
	}

	key = strings.Trim(key, " ")
	if idxDot:=strings.Index(key, "."); idxDot>0 && idxDot<len(key)-1 {
		tableAlias := key[:idxDot]
		columnAlias := key[idxDot+1:]

		vmap,ok := row.filledMap[tableAlias]
		if !ok {
			return nil,false
		}

		switch vmap.(type) {
		case ZMap:
			v,ok := vmap.(ZMap)[columnAlias]
			return v,ok
		default:
			return nil,false
		}
	} else {
		v,ok := row.filledMap[key]

		return v,ok
	}

	return nil,false
}

func (row *zRow) fill(sqlRow zScanner) (error) {
	var sqlRowValue = make([]interface{}, len(row.value))
	var r = reflect.ValueOf(&sqlRowValue).Elem()
	var valuePtr = make([]interface{}, len(sqlRowValue))
	for i:=0; i<len(sqlRowValue); i++ {
		valuePtr[i] = r.Index(i).Addr().Interface()
	}

	err := sqlRow.Scan(valuePtr...)
	switch {
	case err == sql.ErrNoRows:
		row.filledMap = nil
		return nil
	case err!=nil:
		return err
	}

	row.cover(row.value, sqlRowValue)

	row.filledMap = make(ZMap)
	for idx,column := range row.columns {
		if idxAs := strings.Index(strings.ToUpper(column), " AS "); idxAs > 0 {
			alias := strings.Trim(column[idxAs+3:], " ")
			if alias != "" {
				row.filledMap[alias] = row.value[idx]
			}
		} else if idxDot := strings.Index(column, "."); idxDot>0 {
			tableAlias := strings.Trim(column[:idxDot], " ")
			columnAlias := strings.Trim(column[idxDot+1:], " ")
			if tableAlias!="" && columnAlias!="" {
				if _, ok := row.filledMap[tableAlias]; ok {
					row.filledMap[tableAlias].(ZMap)[columnAlias] = row.value[idx]
				} else {
					row.filledMap[tableAlias] = ZMap{columnAlias: row.value[idx]}
				}
			}
		} else {
			row.filledMap[strings.Trim(column, " ")] = row.value[idx]
		}
	}
	return nil
}

func (row *zRow) cover(dest, sqlRowValue []interface{}) {
	if len(sqlRowValue) != len(dest) {
		return
	}

	for i := 0; i < len(sqlRowValue); i++ {
		v := reflect.New(reflect.TypeOf(dest[i]))
		switch sqlRowValue[i].(type) {
		case int64:
			v.Elem().SetInt(sqlRowValue[i].(int64))
			dest[i] = v.Elem().Interface()
		case float64:
			v.Elem().SetFloat(sqlRowValue[i].(float64))
			dest[i] = v.Elem().Interface()
		case bool:
			v.Elem().SetBool(sqlRowValue[i].(bool))
			dest[i] = v.Elem().Interface()
		case []byte:
			switch dest[i].(type) {
			case string:
				v.Elem().SetString(string(sqlRowValue[i].([]byte)))
			case []byte:
				v.Elem().SetBytes(sqlRowValue[i].([]byte))
			}
			dest[i] = v.Elem().Interface()
		case string:
			v.Elem().SetString(sqlRowValue[i].(string))
			dest[i] = v.Elem().Interface()
		case time.Time:
			switch dest[i].(type) {
			case string:
				v.Elem().SetString(sqlRowValue[i].(time.Time).Format("2006-01-02 15:04:05"))
				dest[i] = v.Elem().Interface()
			case time.Time:
				dest[i] = sqlRowValue[i].(time.Time)
			}
		}
	}
}