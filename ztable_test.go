package zorm

import (
	"time"
	"testing"
	"fmt"
)

type zTestTable1 struct {
}

func (test *zTestTable1) Table() string {
	return "test1"
}

func (test *zTestTable1) PrimaryKey() string {
	return "id"
}

func (test *zTestTable1) Columns() *ZColumnList {
	return &ZColumnList{
		"id": int64(0),
		"c1": "string column",
		"c2": int16(10),
		"c3": "2006-01-02 15:04:05",
		"c4": float64(0.0),
	}
}

func (test *zTestTable1) SoftDelete() SoftDelete {
	return nil
}

type zTestTable2 struct {
}

func (test *zTestTable2) Table() string {
	return "test2"
}

func (test *zTestTable2) PrimaryKey() string {
	return "id"
}

func (test *zTestTable2) Columns() *ZColumnList {
	return &ZColumnList{
		"id": int64(0),
		"c1": "string column",
		"c2": int16(10),
		"c3": "2006-01-02 15:04:05",
		"c4": float64(0.0),
	}
}

func (test *zTestTable2) SoftDelete() SoftDelete {
	return &zTestSoftDelete{}
}

type zTestSoftDelete struct {
}

func (test *zTestSoftDelete) Column() string {
	return "delete_time"
}
func (test *zTestSoftDelete) Value() interface{} {
	return "1971-01-01 00:00:00"
}

func (test *zTestSoftDelete) DeleteValue() interface{} {
	return time.Now().Format("2006-01-02 00:00:00")
}

type testColumnStruct struct {
	Id 			int64 		`column:"id"`
	Name 		string 		`column:"name"`
	Address 	string 		`column:"address"`
	Contact 	string 		`column:"contact"`
	Email 		string 		`column:"email"`
}

func TestZJoinTable_Columns(t *testing.T) {
	var joinTable = new(ZJoinTable)
	joinTable.TableReference = new(zTestTable2)
	joinTable.Alias = "t2"

	joinTable.Join(new(zTestTable1), "t1", JoinOn(WhereColumn("t1.id", "=", "t2.id")))
	//joinTable.LeftJoin(joinTable, "t3", JoinOn(WhereColumn("t1.id", "=", "t3.id")))

	fmt.Println(joinTable.Table())
	fmt.Println(joinTable.tableArgs())
}

func TestZColumnList_Bind(t *testing.T) {
	var column = ZColumnList{}
	var tst testColumnStruct

	column.Bind(&tst)

	fmt.Println(column)
}