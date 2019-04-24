package sql4

import (
	"fmt"

	"github.com/yourhe/stores/queryer"
)

type Searcher interface {
	Find(interface{}, interface{}) error
	//支持单个或多个？
}

type SearchOption struct {
	// *queryer.BoolQuery //如何保持子类的链式调用
	query  *queryer.BoolQuery
	limit  int
	offset int
	sort   *SortOpt
}

type SortOpt struct {
	Field   string
	Reverse bool
}

func NewSearchOption() *SearchOption {
	opt := &SearchOption{
		query: queryer.NewBoolQuery(),
	}
	return opt
}

func (opt *SearchOption) Limit(limit int) *SearchOption {
	opt.limit = limit
	return opt
}

func (opt *SearchOption) Offset(offset int) *SearchOption {
	opt.offset = offset
	return opt
}

func (opt *SearchOption) Sort(order string, reverse bool) *SearchOption {
	opt.sort = &SortOpt{order, reverse}
	return opt
}

func (opt *SearchOption) Must(queries ...queryer.Query) *SearchOption {
	opt.query.Must(queries...)
	return opt
}

//Should 相当于 “OR”
func (opt *SearchOption) Should(queries ...queryer.Query) *SearchOption {
	opt.query.Should(queries...)
	return opt
}

func (opt *SearchOption) Source(qt queryer.QueryTransfer) (interface{}, error) {
	sql, err := qt.TransTo(opt.query)
	if err != nil {
		return nil, err
	}
	condSql := sql.(string)
	optSql := ""
	if opt.sort != nil && opt.sort.Field != "" {
		order := "asc"
		if opt.sort.Reverse {
			order = "desc"
		}
		optSql += fmt.Sprintf("order by %s %s", opt.sort.Field, order)
	}
	if opt.limit != 0 {
		optSql += fmt.Sprintf(" limit %d", opt.limit)
	}
	if opt.offset != 0 {
		optSql += fmt.Sprintf(" offset %d", opt.offset)
	}
	if condSql == "" {
		return optSql, nil
	}
	return "where " + condSql + " " + optSql, nil
}
