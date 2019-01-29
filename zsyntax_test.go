package zorm

import (
	"testing"
	"fmt"
	"time"
)

func TestDeleteSyntax(t *testing.T)  {
	var syntax = new(zDelete)
	syntax.table = new(zTestTable1)
	syntax.where = WhereColumn("c1", "=", "test").Where("c2", ">", 3)
	syntax.orderBy = OrderBy("id", "")
	syntax.limit = Limit(1, 0)

	query,args,err := syntax.query()

	if err != nil {
		fmt.Println("ERROR: ", err.Error())
	}

	fmt.Println(query, args)
}

func TestInsertSyntax(t *testing.T) {
	var syntax = new(zInsert)
	syntax.table = new(zTestTable1)
	syntax.assigns = AssignList{"c1":"hhhh", "c2":int16(10), "c3":time.Now().Format("2006-01-02 15:04:05")}

	fmt.Println(syntax.query())
}

func TestUpdateSyntax(t *testing.T)  {
	var syntax = new(zUpdate)
	syntax.table = new(zTestTable1)
	syntax.where = WhereIn("c1", "a", "b", "c", "d").Between("c2", 1, 10).OrWhere(WhereColumn("c1", "=", "dddd"))
	syntax.orderBy = OrderBy("c2", "").OrderBy("c3", "DESC")
	syntax.limit = Limit(10, 0)
	syntax.assigns = AssignList{"c1": "xxxxx","c2": 200}

	fmt.Println(syntax.query())
}

func TestSelectSyntax(t *testing.T)  {
	var syntax = new(zSelect)
	syntax.table = new(zTestTable1)
	syntax.where = WhereLike("c1", "%hh%").Where("c2", "=", 10)
	syntax.groupby = GroupBy("c1", "c2").Having(WhereColumn("c1", "=", 100))
	syntax.orderby = OrderBy("c1", "").OrderBy("c2", "DESC")
	syntax.limit = Limit(100, 10)

	fmt.Println(syntax.query("c1, c2, SUM(c4)"))
}