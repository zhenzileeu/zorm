package zorm

import (
	"testing"
	"fmt"
)

func TestZWhere_Where(t *testing.T) {
	var where = new(zWhere)
	where.Where("c1", "=", "test").Where("c2", ">", 10)

	query, args := where.build()
	fmt.Println(query, args)
}

func TestZWhere_Like(t *testing.T) {
	var where = new(zWhere)
	where.Like("c1", "%pattern%").Like("c2", "'pattern%'")

	query,args := where.build()
	fmt.Println(query, args)
}

func TestZWhere_In(t *testing.T) {
	var where = new(zWhere)

	where.In("c1", 1,2,3,4).In("c2", []interface{}{"h", "b", "c"}...)

	query,args := where.build()
	fmt.Println(query, args)
}

func TestZWhere_Raw(t *testing.T) {
	var where = new(zWhere)

	where.Raw("c1 = ? OR c2 = ?", 12, 29).Raw("c3 = ? OR c4 = ?", 12, 19)

	query, args := where.build()

	fmt.Println(query, args)
}

func TestZWhere_Between(t *testing.T) {
	var where = new(zWhere)

	where.Between("c1", 12, 29).Between("c2", "2006-12-19", "2019-01-29")

	query,args := where.build()
	fmt.Println(query, args)
}

func TestZWhere_AndWhere(t *testing.T) {
	var where = new(zWhere)

	where.Where("c1", "=", 12).AndWhere(new(zWhere).Raw("c1=? OR c2=?", 19, 28))

	query,args := where.build()
	fmt.Println(query, args)
}

func TestZWhere_OrWhere(t *testing.T) {
	var where = new(zWhere)

	where.Raw("c1=? AND c2=?", 12, 29).OrWhere(new(zWhere).Like("c3", "%ddd%").Where("c4", ">", 19))

	query,args := where.build()
	fmt.Println(query, args)
}

func TestZOrderBy_OrderBy(t *testing.T) {
	var orderBy = new(zOrderBy)

	orderBy.OrderBy("c1", "").OrderBy("c2", "DESC").OrderBy("c3", "ASC")

	query,args := orderBy.build()

	fmt.Println(query, args)
}

func TestZGroupBy_GroupBy(t *testing.T) {
	var grouby = new(zGroupBy)

	grouby.GroupBy("c1", "c2", "c3").GroupBy("c4", "c5", "c6")

	fmt.Println(grouby.build())
}

func TestZGroupBy_Having(t *testing.T) {
	var groupby = new(zGroupBy)

	groupby.GroupBy("c1", "c2").Having(WhereColumn("c1", ">", 100))
	fmt.Println(groupby.build())
}

func TestZLimit_Limit(t *testing.T) {
	var limit = new(zLimit)

	limit.Limit(100).Offset(10)

	fmt.Println(limit.build2())
}

func TestZLimit_Offset(t *testing.T) {
	var limit = new(zLimit)

	limit.Limit(100).Offset(10)

	fmt.Println(limit.build())
}