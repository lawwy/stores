package queryer

import (
	"fmt"
	"strings"
	// camelcase "github.com/segmentio/go-camelcase"
)

// "gitlab.iyorhe.com/wfgz/reverseproxy/service/stores/driver"

type KeyValue struct {
	Key   string
	Value interface{}
}

type Filter struct {
	Must   []*KeyValue
	Should []*KeyValue
}

//kvTransfer, nosql key,value
// type kvQueryTransfer struct {
// }

//TransTo ...
// getrole(key)
// new boolquery
// new termquery => {field:id,query:value}
// bq.must(query)
// TransTo -> query
// find(query)
//
//

type Query interface {
	// Source returns the JSON-serializable query request.
	Source(QueryTransfer) (interface{}, error)
}

type QueryTransfer interface {
	// Source returns the JSON-serializable query request.
	TransTo(interface{}) (interface{}, error)
}

// type Inmem struct{}

// func (m *Inmem) TransTo(q interface{}) (interface{}, error) {
// 	// panic("not implemented")
// 	switch v := q.(type) {
// 	case *TermQuery:
// 		camelName := strings.Title(camelcase.Camelcase(v.name))
// 		return &KeyValue{Key: camelName, Value: v.value}, nil
// 	case *MatchQuery:
// 		camelName := strings.Title(camelcase.Camelcase(v.name))
// 		return &KeyValue{Key: camelName, Value: v.text}, nil
// 	case *BoolQuery:
// 		var filter = Filter{}
// 		if len(v.mustClauses) > 0 {
// 			for _, must := range v.mustClauses {
// 				transfered, err := m.TransTo(must)
// 				if err != nil {
// 					continue
// 				}
// 				filter.Must = append(filter.Must, transfered.(*KeyValue))
// 			}
// 		}
// 		if len(v.shouldClauses) > 0 {
// 			for _, should := range v.shouldClauses {
// 				transfered, err := m.TransTo(should)
// 				if err != nil {
// 					continue
// 				}
// 				filter.Should = append(filter.Should, transfered.(*KeyValue))
// 			}
// 		}

// 		return &filter, nil
// 	}
// 	return nil, nil
// }

// var defaultTransfer = &Inmem{}

type Sql struct{}

func (s *Sql) TransTo(q interface{}) (interface{}, error) {
	var source string
	switch v := q.(type) {
	case *TermQuery:
		var val string
		switch v.value.(type) {
		case int, int32, int64:
			val = fmt.Sprintf("%v", v.value)
		default:
			val = fmt.Sprintf("'%s'", v.value)
		}
		source = fmt.Sprintf("%s = %s", v.name, val)
	case *MatchQuery:
		var val = []string{}
		switch t := v.text.(type) {
		case string:
			terms := strings.Split(t, " ")
			for _, term := range terms {
				if strings.TrimSpace(term) != "" {
					tmp := fmt.Sprintf("'%%%s%%'", term)
					val = append(val, fmt.Sprintf("%s like %s", v.name, tmp))
				}
			}
		}
		var operator = " or "
		if strings.ToLower(v.operator) == "and" {
			operator = fmt.Sprintf(" %s ", strings.ToLower(v.operator))
		}
		source = strings.Join(val, operator)

		if len(val) > 1 {
			source = fmt.Sprintf("(%s)", source)
		}

	case *BoolQuery:
		var sourceMust string
		if len(v.mustClauses) > 0 {
			var ary = []string{}
			for _, must := range v.mustClauses {
				transfered, err := s.TransTo(must)
				if err != nil {
					continue
				}
				ary = append(ary, transfered.(string))
			}
			sourceMust = strings.Join(ary, " and ")
			if len(ary) > 1 {
				sourceMust = fmt.Sprintf("(%s)", sourceMust)
			}
			source = fmt.Sprintf("%s", sourceMust)
		}

		var sourceShould string

		if len(v.shouldClauses) > 0 {
			var ary = []string{}
			for _, should := range v.shouldClauses {
				transfered, err := s.TransTo(should)
				if err != nil {
					continue
				}

				ary = append(ary, transfered.(string))
			}
			sourceShould = strings.Join(ary, " or ")
			if len(ary) > 1 {
				sourceShould = fmt.Sprintf("(%s)", sourceShould)
			}

			if source == "" {
				source = fmt.Sprintf("%s", sourceShould)

			} else {
				source = fmt.Sprintf("%s or %s", source, sourceShould)

			}

			if sourceMust != "" && sourceShould != "" {
				source = fmt.Sprintf("(%s)", source)
			}
			// fmt.Println(source)

		}

	}
	return source, nil
}
