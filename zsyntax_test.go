package zorm

import (
	"testing"
	"fmt"
	"time"
)

func TestDeleteSyntax(t *testing.T)  {
	var queryBuilder = new(zQueryBuilder)
	queryBuilder.OrderBy("id", "").Limit(1)

	var syntax = new(zDelete)
	syntax.table = new(zTestTable1)
	syntax.where = WhereColumn("c1", "=", "test").Where("c2", ">", 3)
	syntax.orderBy = queryBuilder.orderBy
	syntax.limit = queryBuilder.limit

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
	var qb  = new(zQueryBuilder)
	qb.OrderBy("c2", "").OrderBy("c3", "DESC").Limit(10)

	var syntax = new(zUpdate)
	syntax.table = new(zTestTable1)
	syntax.where = WhereIn("c1", "a", "b", "c", "d").Between("c2", 1, 10).OrWhere(WhereColumn("c1", "=", "dddd"))
	syntax.orderBy = qb.orderBy
	syntax.limit = qb.limit
	syntax.assigns = AssignList{"c1": "xxxxx","c2": 200}

	fmt.Println(syntax.query())
}

func TestSelectSyntax(t *testing.T)  {
	var qb  = new(zQueryBuilder)
	qb.GroupBy("c1", "c2").Having(WhereColumn("c1", "=", 100)).OrderBy("c1", "").OrderBy("c2", "DESC").Paginate(10, 100)

	var syntax = new(zSelect)
	syntax.table = new(zTestTable1)
	syntax.where = WhereLike("c1", "%hh%").Where("c2", "=", 10)
	syntax.groupby = qb.groupBy
	syntax.orderby = qb.orderBy
	syntax.limit = qb.limit

	fmt.Println(syntax.query("c1, c2, SUM(c4)"))
}