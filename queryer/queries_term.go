package queryer

// TermQuery finds documents that contain the exact term specified
// in the inverted index.
type TermQuery struct {
	name      string
	value     interface{}
	queryName string
}

// NewTermQuery creates and initializes a new TermQuery.
func NewTermQuery(name string, value interface{}) *TermQuery {
	return &TermQuery{name: name, value: value}
}

// QueryName sets the query name for the filter that can be used
// when searching for matched_filters per hit
func (q *TermQuery) QueryName(queryName string) *TermQuery {
	q.queryName = queryName
	return q
}

// Source returns JSON for the query.
func (q *TermQuery) Source2() (interface{}, error) {
	// {"term":{"name":"value"}}
	source := make(map[string]interface{})
	// tq := make(map[string]interface{})
	// source["term"] = tq

	// if q.boost == nil && q.queryName == "" {
	// 	tq[q.name] = q.value
	// } else {
	// 	subQ := make(map[string]interface{})
	// 	subQ["value"] = q.value
	// 	if q.boost != nil {
	// 		subQ["boost"] = *q.boost
	// 	}
	// 	if q.queryName != "" {
	// 		subQ["_name"] = q.queryName
	// 	}
	// 	tq[q.name] = subQ
	// }
	return source, nil
}

func (q *TermQuery) Source(qt QueryTransfer) (interface{}, error) {
	return qt.TransTo(q)
	// panic("not implated")
}
