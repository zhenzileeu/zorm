package zorm

import (
	"testing"
	"fmt"
)

func TestWhereColumn(t *testing.T) {
	var where = WhereColumn("c1", "=", 100).Where("c2", ">", 10)
	fmt.Println(where.build())
}

func TestWhereRaw(t *testing.T) {
	var where = WhereRaw("c1 = ? and c2 like '%ddd%'", 122)
	fmt.Println(where.build())
}

func TestWhereBetween(t *testing.T) {
	var where = WhereBetween("c1", 100, 2000)
	fmt.Println(where.build())
}

func TestWhereLike(t *testing.T) {
	var where = WhereLike("c1", "%dddd")
	fmt.Println(where.build())
}

func TestWhereIn(t *testing.T) {
	var where = WhereIn("c1", "d", "ddd", "DDDDD")
	fmt.Println(where.build())
}