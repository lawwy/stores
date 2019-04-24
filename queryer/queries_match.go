package queryer

// MatchQuery is a family of queries that accepts text/numerics/dates,
// analyzes them, and constructs a query.
type MatchQuery struct {
	name      string
	text      interface{}
	operator  string // or / and
	analyzer  string
	queryName string
}

// NewMatchQuery creates and initializes a new MatchQuery.
func NewMatchQuery(name string, text interface{}) *MatchQuery {
	return &MatchQuery{name: name, text: text}
}

// Operator sets the operator to use when using a boolean query.
// Can be "AND" or "OR" (default).
func (mq *MatchQuery) Operator(operator string) *MatchQuery {
	mq.operator = operator
	return mq
}

// Source returns JSON for the function score query.
func (mq *MatchQuery) Source(qt QueryTransfer) (interface{}, error) {
	return qt.TransTo(mq)
}
