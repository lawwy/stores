package queryer

//BoolQuery 逻辑组合查询
type BoolQuery struct {
	mustClauses    []Query
	mustNotClauses []Query
	filterClauses  []Query
	shouldClauses  []Query
	transfer       QueryTransfer
}

//NewBoolQuery Creates a new bool query.
func NewBoolQuery() *BoolQuery {
	return &BoolQuery{
		mustClauses:    make([]Query, 0),
		mustNotClauses: make([]Query, 0),
		filterClauses:  make([]Query, 0),
		shouldClauses:  make([]Query, 0),
	}
}

//Must 相当于 “AND”
func (q *BoolQuery) Must(queries ...Query) *BoolQuery {
	q.mustClauses = append(q.mustClauses, queries...)
	return q
}

//Should 相当于 “OR”
func (q *BoolQuery) Should(queries ...Query) *BoolQuery {
	q.shouldClauses = append(q.shouldClauses, queries...)
	return q
}

func (b *BoolQuery) Source(qt QueryTransfer) (interface{}, error) {
	return qt.TransTo(b)
}

func (b *BoolQuery) GetMust() []Query {
	return b.mustClauses
}

func (b *BoolQuery) GetShould() []Query {
	return b.shouldClauses
}
